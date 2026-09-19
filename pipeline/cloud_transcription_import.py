"""Offline, exact-identity admission of independently supplied transcripts.

Nothing in this module writes files, reads media, calls an API, or replaces an
existing transcript. A caller must retain ``read_raw(entry)`` unchanged alongside
the returned normalized document and provenance receipt. Filename matching is
case-sensitive: YouTube identifiers and literal Archive filenames only, never
fuzzy titles, dates, inferred short-hash recipes, or speaker identities.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import parse_qs, urlsplit, unquote


class ImportError(RuntimeError):
    """A source cannot safely be admitted as a complete transcript."""


SOURCE_URL = "https://old.reddit.com/r/HIMRFAM2/comments/1weonll/himr_transcripts_2015september_2026_excluding/"
MAX_FILE_BYTES = 32 * 1024**2
MAX_TOTAL_BYTES = 1024**3
MAX_FILES = 20000
MAX_SEGMENTS = 100000
MAX_TEXT_CHARACTERS = 16 * 1024**2
MAX_TIME_MS = 366 * 24 * 3600 * 1000
YT_ID = re.compile(r"[A-Za-z0-9_-]{11}\Z")
STAMP = r"(\d{2,4}):([0-5]\d):([0-5]\d)[,.](\d{3})"
TIMING = re.compile(r"^" + STAMP + r"[ \t]+-->[ \t]+" + STAMP + r"[ \t]*$")
DATE_PREFIX = re.compile(r"^(?:\d{4}-\d{2}-\d{2}|undated) - ")
BRACKET_ID = re.compile(r" \[([^\[\]]+)\]$")
IA_ID = re.compile(r"IA-([A-Za-z0-9_.-]+)-([0-9a-f]{12})\Z")
SPEAKER = re.compile(r"^(?:\[(?P<bracket>(?:Speaker|SPEAKER)[ _-]?[A-Za-z0-9]+)\]|(?P<colon>(?:Speaker|SPEAKER)[ _-]?[A-Za-z0-9]+):)[ \t]*(?P<text>[\s\S]*)$")


def _canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def _sha(body):
    return hashlib.sha256(body).hexdigest()


def _path(value):
    path = Path(value).absolute()
    if ".." in path.parts:
        raise ImportError("transcript path contains parent traversal")
    # Reject symlinked ancestors as well as the final file. This is a local
    # provenance importer, not a mechanism for following external references.
    for part in (*reversed(path.parents), path):
        try:
            observed = part.lstat()
        except OSError as exc:
            raise ImportError(f"cannot inspect transcript path: {part}") from exc
        if stat.S_ISLNK(observed.st_mode):
            raise ImportError("transcript path contains a symlink")
    return path


def _witness(observed):
    return (observed.st_dev, observed.st_ino, observed.st_size, observed.st_mtime_ns,
            observed.st_ctime_ns, observed.st_mode)


def _read(path):
    path = _path(path)
    directory = None
    try:
        # Retain each parent descriptor while opening its child without following
        # symlinks; a directory rename cannot redirect the checked final open.
        directory = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= MAX_FILE_BYTES:
                raise ImportError("transcript is not a bounded regular file")
            chunks, count = [], 0
            while count <= MAX_FILE_BYTES:
                block = os.read(fd, min(1024**2, MAX_FILE_BYTES + 1 - count))
                if not block:
                    break
                chunks.append(block)
                count += len(block)
            if count != before.st_size or _witness(os.fstat(fd)) != _witness(before):
                raise ImportError("transcript changed while being read")
            return b"".join(chunks)
        finally:
            os.close(fd)
    except OSError as exc:
        raise ImportError(f"cannot read transcript: {path}") from exc
    finally:
        if directory is not None:
            os.close(directory)


def _video_ids(stem):
    if stem.endswith(".ia"):
        stem = stem[:-3]
    ids = set()
    bracket = BRACKET_ID.search(stem)
    if bracket and YT_ID.fullmatch(bracket[1]):
        ids.add(bracket[1])
    trailing = re.search(r"-([A-Za-z0-9_-]{11})$", stem)
    if trailing:
        ids.add(trailing[1])
    return ids


def identity_keys(filename):
    """Interpret only the export's explicit ID and literal source-filename fields."""
    stem = Path(filename).stem
    dated = re.match(r"^(\d{4}-\d{2}-\d{2}) - ", stem)
    calendar = None
    if dated:
        try:
            calendar = date.fromisoformat(dated[1]).strftime("%Y%m%d")
        except ValueError:
            pass
    stem = DATE_PREFIX.sub("", stem, count=1)
    export_basename = stem
    bracket = BRACKET_ID.search(stem)
    ids, keys = set(), set()
    if bracket:
        export_id = bracket[1]
        stem = stem[:bracket.start()]
        if YT_ID.fullmatch(export_id):
            ids.add(export_id)
            # Unlike the opaque export IDs, a bracketed YouTube ID is part of
            # the literal source filename in the retained yt-dlp-style archive.
            # Compare that full basename, not a title field. Retain competing
            # physical media and reject global filename/source collisions.
            keys.add("media_stem:" + export_basename)
            if dated and calendar:
                keys.add("media_stem:[" + dated[1] + "] " + export_basename)
        elif ia := IA_ID.fullmatch(export_id):
            # The 12-character hash is opaque. The adjacent original filename
            # and explicit Archive item are usable without guessing its recipe.
            # The export splits a native YYYYMMDD- prefix into its explicit
            # calendar-date field. Expand only those lossless known filename
            # forms; all title characters remain exact and case-sensitive.
            # Internet Archive's .ia media derivative is likewise explicit.
            stems = {stem, stem + ".ia"}
            if calendar:
                stems.update({calendar + "-" + stem, calendar + "-" + stem + ".ia"})
            keys.update("archive_stem:" + ia[1] + "/" + value for value in stems)
        elif re.fullmatch(r"[0-9a-f]{20}", export_id):
            # These exports preserve the literal Archive source filename. This
            # key must be unique across the complete recording inventory before
            # batch use (see match_recordings); never compare display titles.
            keys.add("media_stem:" + stem)
    ids.update(_video_ids(stem))
    keys.update("youtube:" + value for value in ids)
    return sorted(keys)


def recording_keys(recording):
    """Accept existing Archive inventory rows or explicit native-ID metadata."""
    if not isinstance(recording, dict):
        raise ImportError("recording metadata must be an object")
    aliases = recording.get("aliases", [])
    if not isinstance(aliases, list) or len(aliases) > 1000:
        raise ImportError("recording aliases exceed their bound")
    keys = set()
    source_ids = recording.get("source_ids", {})
    if not isinstance(source_ids, dict):
        raise ImportError("recording source IDs must be an object")
    explicit = []
    for field, target in (("youtube", "youtube_id"), ("archive_native", "native_id")):
        values = source_ids.get(field, [])
        if not isinstance(values, list) or len(values) > 1000:
            raise ImportError("recording source ID list exceeds its bound")
        explicit.extend({target: value} for value in values)
    for item in [recording, *aliases, *explicit]:
        if not isinstance(item, dict):
            raise ImportError("recording alias must be an object")
        video = item.get("youtube_id")
        if video is not None:
            if not isinstance(video, str) or not YT_ID.fullmatch(video):
                raise ImportError("invalid explicit YouTube ID")
            keys.add("youtube:" + video)
        for field in ("source_native_id", "native_id"):
            native = item.get(field)
            if native is None:
                continue
            if not isinstance(native, str) or len(native) > 8192:
                raise ImportError("invalid source native ID")
            if "/" in native:
                archive, filename = native.split("/", 1)
                if archive and filename and not filename.startswith("/") and ".." not in Path(filename).parts:
                    stem = str(Path(filename).with_suffix(""))
                    keys.add("archive_stem:" + archive + "/" + stem)
                    keys.add("media_stem:" + stem)
                    keys.update("youtube:" + value for value in _video_ids(Path(filename).stem))
            elif YT_ID.fullmatch(native):
                keys.add("youtube:" + native)
        for field in ("canonical_url", "url"):
            url = item.get(field)
            if not isinstance(url, str) or len(url) > 8192:
                continue
            parsed = urlsplit(url)
            hostname = (parsed.hostname or "").lower()
            if hostname in {"www.youtube.com", "youtube.com", "m.youtube.com"}:
                video = parse_qs(parsed.query).get("v", [None])[0]
            elif hostname == "youtu.be":
                video = parsed.path.strip("/")
            else:
                video = None
            if isinstance(video, str) and YT_ID.fullmatch(video):
                keys.add("youtube:" + video)
            if hostname in {"archive.org", "www.archive.org"} and parsed.path.startswith("/download/"):
                native = unquote(parsed.path[len("/download/"):])
                keys.update(recording_keys({"native_id": native}))
    return sorted(keys)


def _ms(groups):
    hour, minute, second, millisecond = map(int, groups)
    value = ((hour * 60 + minute) * 60 + second) * 1000 + millisecond
    if value > MAX_TIME_MS:
        raise ImportError("timestamp exceeds supported range")
    return value


def parse_transcript(body):
    """Parse bounded SRT without dropping malformed suffixes or inventing times."""
    if not isinstance(body, bytes) or len(body) > MAX_FILE_BYTES:
        raise ImportError("transcript exceeds its byte bound")
    try:
        text = body.decode("utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
    except UnicodeError as exc:
        raise ImportError("transcript is not UTF-8") from exc
    if any(ord(ch) < 32 and ch not in "\n\t" for ch in text):
        raise ImportError("transcript contains control characters")
    if not text.strip():
        return {"format": "empty", "segments": [], "speaker_labels": {}, "issues": ["empty_transcript"]}
    if "-->" not in text:
        if len(text) > MAX_TEXT_CHARACTERS:
            raise ImportError("transcript text exceeds its character bound")
        return {"format": "plain_text", "segments": [{"start_ms": None, "end_ms": None,
            "text": text.strip(), "speaker": None}], "speaker_labels": {},
            "issues": ["unstructured_text_requires_review"]}
    blocks = re.split(r"\n[ \t]*\n", text.strip())
    if len(blocks) > MAX_SEGMENTS:
        raise ImportError("transcript cue count exceeds its bound")
    segments, speakers, issues, total_text = [], {}, [], 0
    prior_start, prior_number = -1, 0
    for block in blocks:
        lines = block.split("\n")
        if len(lines) < 3 or not re.fullmatch(r"[0-9]{1,8}", lines[0].strip()):
            raise ImportError("malformed or truncated SRT cue")
        number = int(lines[0])
        if number != prior_number + 1:
            issues.append("nonconsecutive_cue_numbers")
        prior_number = number
        timing = TIMING.fullmatch(lines[1])
        if not timing:
            raise ImportError("invalid SRT timestamp line")
        start, end = _ms(timing.groups()[:4]), _ms(timing.groups()[4:])
        if end <= start:
            raise ImportError("SRT cue end must follow its start")
        if start < prior_start:
            issues.append("nonmonotonic_cue_start")
        prior_start = start
        content = "\n".join(lines[2:]).strip()
        if not content or "-->" in content:
            raise ImportError("empty or malformed SRT cue text")
        speaker = None
        if named := SPEAKER.fullmatch(content):
            label = named["bracket"] or named["colon"]
            if label not in speakers:
                speakers[label] = "SPEAKER_" + str(len(speakers)).zfill(4)
            speaker, content = speakers[label], named["text"]
            if not content.strip():
                raise ImportError("speaker cue has no text")
        total_text += len(content)
        if total_text > MAX_TEXT_CHARACTERS:
            raise ImportError("transcript text exceeds its character bound")
        segments.append({"start_ms": start, "end_ms": end, "text": content, "speaker": speaker})
    return {"format": "srt", "segments": segments, "speaker_labels": speakers,
            "issues": sorted(set(issues))}


def inventory(root):
    """Read only supplied transcript files; return stable metadata, not full text."""
    root = _path(root)
    if not root.is_dir():
        raise ImportError("transcript inventory root is not a directory")
    paths = []
    def walk_error(error):
        raise ImportError("cannot inspect complete transcript directory inventory") from error
    for directory, names, files in os.walk(root, followlinks=False, onerror=walk_error):
        if any((Path(directory) / name).is_symlink() for name in names):
            raise ImportError("transcript inventory contains a symlinked directory")
        for filename in sorted(files):
            path = Path(directory) / filename
            if path.suffix.lower() in {".txt", ".srt"}:
                paths.append(path)
            if len(paths) > MAX_FILES:
                raise ImportError("transcript inventory exceeds file bound")
    entries, total = [], 0
    for path in sorted(paths):
        raw = _read(path)
        total += len(raw)
        if total > MAX_TOTAL_BYTES:
            raise ImportError("transcript inventory exceeds total byte bound")
        entry = {"path": str(path), "sha256": _sha(raw), "byte_count": len(raw),
                 "identity_keys": identity_keys(path.name)}
        try:
            parsed = parse_transcript(raw)
            segments = parsed["segments"]
            ends = [row["end_ms"] for row in segments if row["end_ms"] is not None]
            entry.update({"format": parsed["format"], "segment_count": len(segments),
                "text_characters": sum(len(row["text"]) for row in segments),
                "first_start_ms": segments[0]["start_ms"] if segments else None,
                "last_end_ms": max(ends) if ends else None,
                "content_sha256": _sha(_canonical(segments)),
                "speaker_label_count": len(parsed["speaker_labels"]), "issues": parsed["issues"]})
        except ImportError as exc:
            entry.update({"format": "invalid", "segment_count": 0, "text_characters": 0,
                "first_start_ms": None, "last_end_ms": None, "content_sha256": None,
                "speaker_label_count": 0, "issues": ["malformed_transcript"], "error": str(exc)})
        entry["status"] = "review_required" if entry["issues"] else "eligible"
        entries.append(entry)
    return entries


def _duration(recording):
    nested = recording.get("recording", {})
    duration = recording.get("duration_ms", recording.get("duration_hint_ms",
                           nested.get("duration_hint_ms") if isinstance(nested, dict) else None))
    if duration is not None and (type(duration) is not int or not 0 < duration <= MAX_TIME_MS):
        raise ImportError("invalid recording duration")
    return duration


def _coverage_issues(entry, duration):
    if duration is not None and (type(duration) is not int or not 0 < duration <= MAX_TIME_MS):
        raise ImportError("invalid recording duration")
    if duration is None or entry.get("last_end_ms") is None:
        return []
    end = entry["last_end_ms"]
    issues = []
    if end > duration + max(10000, duration // 100):
        issues.append("timestamps_exceed_media_duration")
    if duration - end > max(120000, duration // 10):
        issues.append("possible_missing_tail")
    return issues


def index_entries(entries):
    """Build the reusable metadata-only exact-key index for a full archive scan."""
    index = {}
    if not isinstance(entries, list) or len(entries) > MAX_FILES:
        raise ImportError("transcript entry list exceeds its bound")
    for entry in entries:
        for key in entry["identity_keys"]:
            index.setdefault(key, []).append(entry)
    return index


def match(recording, entries):
    """Select the strongest exact identity; contradictory peers are review holds.

    A literal item+filename is more specific than a video ID reused across edits
    or encodings. We retain lower-specificity candidates for the caller's audit,
    but do not let them override an exact source-file binding. Batch collision
    checks still reject even a strong key when two different recordings own it.
    """
    keys = set(recording_keys(recording))
    indexed = index_entries(entries) if isinstance(entries, list) else entries
    if not isinstance(indexed, dict):
        raise ImportError("transcript entries must be a list or exact-key index")
    by_path = {entry["path"]: entry for key in keys for entry in indexed.get(key, [])}
    all_candidates = [by_path[path] for path in sorted(by_path)]
    priority = {"archive_stem": 3, "media_stem": 2, "youtube": 1}
    common = {key for entry in all_candidates for key in entry["identity_keys"] if key in keys}
    strongest = max((priority[key.split(":", 1)[0]] for key in common), default=0)
    matched_keys = {key for key in common if priority[key.split(":", 1)[0]] == strongest}
    candidates = [entry for entry in all_candidates if matched_keys.intersection(entry["identity_keys"])]
    result = {"status": "missing", "selected": None, "candidates": candidates,
              "matched_keys": sorted(matched_keys),
              "lower_specificity_candidates": [entry for entry in all_candidates if entry not in candidates],
              "selection_policy": "strongest_exact_source_binding_v1",
              "matching_basis": sorted({{"archive_stem": "literal_archive_source_filename",
                                           "media_stem": "literal_export_basename",
                                           "youtube": "exact_youtube_id"}[key.split(":", 1)[0]]
                                          for key in matched_keys}),
              "issues": [], "full_media_coverage_verified": False}
    if not candidates:
        return result
    # Byte-identical copies and equivalent SRT line endings can safely converge;
    # different words, timestamps or labels are conflicts, never a priority tie.
    identities = {entry.get("content_sha256") for entry in candidates}
    if len(identities) != 1 or None in identities:
        result.update(status="ambiguous" if len(candidates) > 1 else "review_required",
                      issues=["conflicting_or_invalid_transcript_candidates"])
        return result
    selected = min(candidates, key=lambda entry: entry["path"])
    issues = sorted({issue for entry in candidates for issue in entry["issues"]}
                    | set(_coverage_issues(selected, _duration(recording))))
    result.update(status="review_required" if issues else "selected", selected=selected, issues=issues)
    return result


def match_recordings(recordings, entries):
    """Batch matching adds cross-recording key-collision protection."""
    owners, indexed = {}, index_entries(entries)
    for index, recording in enumerate(recordings):
        for key in recording_keys(recording):
            owners.setdefault(key, set()).add(index)
    results = []
    for recording in recordings:
        result = match(recording, indexed)
        related = {owner for key in recording_keys(recording) if key.startswith("youtube:")
                   for owner in owners[key]}
        result["related_physical_recordings"] = sorted({
            recordings[owner].get("recording_id", recordings[owner].get("recording", {}).get("media_id",
                                 f"inventory-row-{owner:05d}"))
            for owner in related})
        # A video ID may label different archive edits as well as duplicate
        # encodings. Without explicit equivalence evidence, hold all collisions.
        collisions = [key for key in result["matched_keys"] if len(owners[key]) > 1]
        if collisions:
            result.update(status="ambiguous", selected=None,
                          issues=sorted(set(result["issues"] + ["identity_key_matches_multiple_recordings"])))
        results.append(result)
    selected_owners = {}
    for index, result in enumerate(results):
        if result["selected"] is not None:
            selected_owners.setdefault(result["selected"]["path"], set()).add(index)
    for index, result in enumerate(results):
        if (result["selected"] is not None
                and len(selected_owners[result["selected"]["path"]]) > 1):
            result.update(status="ambiguous", selected=None,
                issues=sorted(set(result["issues"] + ["transcript_source_matches_multiple_recordings"])))
    return results


def read_raw(entry):
    """Return exact original bytes after checking the inventory's binding."""
    raw = _read(entry["path"])
    if _sha(raw) != entry["sha256"] or len(raw) != entry["byte_count"]:
        raise ImportError("third-party transcript changed since inventory")
    return raw


def normalize(entry, recording_id, *, duration_ms=None):
    """Return an existing-summary-compatible document plus a provenance receipt.

    Review holds cannot be bypassed here. Resolve them with an explicit revised
    source or separately audited mapping policy instead of changing source bytes.
    Full-media coverage and model identity are not independently verified.
    """
    if not isinstance(recording_id, str) or not recording_id.strip() or len(recording_id) > 256:
        raise ImportError("invalid recording ID")
    raw = read_raw(entry)
    parsed = parse_transcript(raw)
    fresh_end = max((row["end_ms"] for row in parsed["segments"] if row["end_ms"] is not None), default=None)
    if fresh_end != entry["last_end_ms"] or parsed["issues"] != entry["issues"]:
        raise ImportError("third-party inventory quality metadata differs")
    issues = parsed["issues"] + _coverage_issues({"last_end_ms": fresh_end}, duration_ms)
    if issues or not parsed["segments"]:
        raise ImportError("third-party transcript requires review: " + ", ".join(issues))
    segments = parsed["segments"]
    if _sha(_canonical(segments)) != entry["content_sha256"]:
        raise ImportError("third-party inventory normalization differs")
    receipt = {"kind": "himr_cloud_third_party_import_provenance", "schema_version": 1,
        "recording_id": recording_id, "original": {key: entry[key] for key in ("path", "sha256", "byte_count")},
        "source_url": SOURCE_URL, "attribution": "u/MelatoninHighs",
        "model": "Universal-3.5 Pro", "provider": "AssemblyAI",
        "model_provenance": "user_attested_author_confirmation", "model_independently_verified": False,
        "machine_generated": True, "full_media_coverage_verified": False,
        "verified_quotation": False, "speaker_identity_inferred": False,
        "speaker_label_mapping": parsed["speaker_labels"], "source_format": parsed["format"],
        "source_modified": False, "rights_granted": False}
    doc = {"kind": "himr_third_party_transcript_import", "schema_version": 1,
        "recording_id": recording_id, "status": "completed", "segments": segments,
        "provenance": {"label": "HIMR-Transcripts; machine-generated Universal-3.5 Pro (user-attested author confirmation, not independently verified)",
            "source_url": SOURCE_URL,
            "attribution": "u/MelatoninHighs; original SHA-256 " + entry["sha256"],
            "rights_note": "No license or publication rights inferred; ASR text and complete media coverage remain unverified."}}
    return {"transcript": doc, "provenance": receipt}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--recordings", help="existing Archive inventory JSON; read-only")
    args = parser.parse_args(argv)
    entries = inventory(args.root)
    result = {"kind": "himr_cloud_third_party_inventory", "schema_version": 1,
              "entries": entries, "counts": dict(Counter(entry["status"] for entry in entries)),
              "total_bytes": sum(entry["byte_count"] for entry in entries)}
    if args.recordings:
        archive = json.loads(_read(args.recordings))
        recordings = archive.get("recordings", archive.get("records"))
        if not isinstance(recordings, list):
            raise ImportError("archive inventory lacks recording rows")
        matches = match_recordings(recordings, entries)
        result["match_counts"] = dict(Counter(row["status"] for row in matches))
        result["matches"] = [{"recording_id": recording.get("recording_id",
                                recording.get("recording", {}).get("media_id")), **row}
                             for recording, row in zip(recordings, matches)]
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
