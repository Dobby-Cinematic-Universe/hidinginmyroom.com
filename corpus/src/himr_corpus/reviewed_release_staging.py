"""Build an owner-private release candidate from reviewed manifests.

This module is intentionally separate from the ordinary catalog CLI.  It copies a
sealed, hash-pinned catalog backup into a private run directory and performs every
write against that disposable copy.  It never opens the supplied base catalog with
SQLite and it has no operation that installs a release under the site's public data
tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Any, BinaryIO

from .db import connect, migrate
from .exporter import build_release
from .machine_transcript_publication import (
    DISCLAIMER_CODE,
    DISCLAIMER_TEXT,
    MachineTranscriptPublicationPolicyError,
    apply_machine_transcript_publication_plan,
    build_machine_transcript_publication_plan,
)
from .publication_admin import (
    PublicationManifest,
    PublicationManifestError,
    apply_publication_manifest,
    load_publication_manifest,
)
from .reviewer_admin import (
    ReviewerAdminManifest,
    ReviewerAdminManifestError,
    apply_reviewer_admin_manifest,
    load_reviewer_admin_manifest,
)
from .sharded_release import export_release_v2_from_release, validate_sharded_release
from .validation import validate_database


SCHEMA_VERSION = 1
PACKET_KIND = "first_public_machine_transcript_review_evidence"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PLACEHOLDER_PREFIX = "REPLACE_WITH"
MAX_JSON_BYTES = 16 * 1024 * 1024
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
FORBIDDEN_OUTPUT_ROOTS = tuple(
    REPOSITORY_ROOT / name for name in ("src", "public", "dist", ".git")
)
ALLOWED_OUTPUT_ROOTS = (REPOSITORY_ROOT / "research" / "corpus", Path("/tmp"))


class ReviewedReleaseStagingError(ValueError):
    """A staging input or requested operation violates the release boundary."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def _sha256_stream(
    handle: BinaryIO, *, copy_to: BinaryIO | None = None
) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    while True:
        block = handle.read(1024 * 1024)
        if not block:
            break
        digest.update(block)
        byte_count += len(block)
        if copy_to is not None:
            copy_to.write(block)
    return digest.hexdigest(), byte_count


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IFMT(value.st_mode),
        stat.S_IMODE(value.st_mode),
        value.st_nlink,
    )


def _regular_nonsymlink(path: Path, label: str) -> os.stat_result:
    try:
        before = path.lstat()
    except OSError as error:
        raise ReviewedReleaseStagingError(f"{label} is unavailable: {error}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ReviewedReleaseStagingError(
            f"{label} must be a regular non-symlink file"
        )
    return before


def _stable_read(path: Path, label: str, *, maximum: int = MAX_JSON_BYTES) -> bytes:
    path_before = _regular_nonsymlink(path, label)
    path = path.resolve(strict=True)
    try:
        with path.open("rb") as handle:
            descriptor_before = os.fstat(handle.fileno())
            if _identity(descriptor_before) != _identity(path_before):
                raise ReviewedReleaseStagingError(
                    f"{label} was replaced while being opened"
                )
            if descriptor_before.st_size > maximum:
                raise ReviewedReleaseStagingError(
                    f"{label} exceeds the {maximum}-byte limit"
                )
            body = handle.read()
            descriptor_after = os.fstat(handle.fileno())
    except OSError as error:
        raise ReviewedReleaseStagingError(f"cannot read {label}: {error}") from error
    path_after = path.stat()
    if (
        _identity(descriptor_before) != _identity(descriptor_after)
        or _identity(descriptor_before) != _identity(path_after)
        or len(body) != descriptor_before.st_size
    ):
        raise ReviewedReleaseStagingError(f"{label} changed while being read")
    return body


def _json_without_duplicate_keys(body: bytes, label: str) -> object:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ReviewedReleaseStagingError(
                    f"{label} contains duplicate JSON key {key!r}"
                )
            result[key] = value
        return result

    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReviewedReleaseStagingError(f"{label} is not valid UTF-8 JSON") from error


def _reject_placeholders(value: object, label: str = "manifest") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            _reject_placeholders(child, f"{label}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_placeholders(child, f"{label}[{index}]")
    elif isinstance(value, str) and PLACEHOLDER_PREFIX in value:
        raise ReviewedReleaseStagingError(
            f"{label} still contains the template marker {PLACEHOLDER_PREFIX!r}"
        )


def _required_sha256(value: str, label: str) -> str:
    if not SHA256_RE.fullmatch(value):
        raise ReviewedReleaseStagingError(
            f"{label} must be 64 lowercase hexadecimal characters"
        )
    return value


def _load_pinned_json(
    path: Path, expected_sha256: str, label: str
) -> tuple[dict[str, Any], bytes]:
    expected_sha256 = _required_sha256(expected_sha256, f"{label} SHA-256")
    body = _stable_read(path, label)
    observed = hashlib.sha256(body).hexdigest()
    if observed != expected_sha256:
        raise ReviewedReleaseStagingError(
            f"{label} SHA-256 mismatch: expected {expected_sha256}, observed {observed}"
        )
    value = _json_without_duplicate_keys(body, label)
    if not isinstance(value, dict):
        raise ReviewedReleaseStagingError(f"{label} must be a JSON object")
    return value, body


def _packet_expectation(evidence: dict[str, Any]) -> dict[str, Any]:
    if evidence.get("schema_version") != 1 or evidence.get("packet_kind") != PACKET_KIND:
        raise ReviewedReleaseStagingError(
            "evidence is not a supported first-public-release review packet"
        )
    try:
        source_id = evidence["source"]["source_id"]
        recording_id = evidence["recording"]["recording_id"]
        revision = evidence["transcript_revision"]
        revision_id = revision["revision_id"]
        segments = revision["segments"]
        expected = evidence["release_expectation_after_valid_decisions"]
    except (KeyError, TypeError) as error:
        raise ReviewedReleaseStagingError(
            "evidence lacks the candidate or release expectation"
        ) from error
    for value, label in (
        (source_id, "source_id"),
        (recording_id, "recording_id"),
        (revision_id, "revision_id"),
    ):
        if not isinstance(value, str) or not value:
            raise ReviewedReleaseStagingError(f"evidence {label} is invalid")
    if not isinstance(segments, list) or not segments:
        raise ReviewedReleaseStagingError("evidence must name at least one segment")
    segment_count = len(segments)
    required_counts = {
        "sources": 1,
        "recordings": 1,
        "transcript_revisions": 1,
        "segments": segment_count,
    }
    if any(expected.get(key) != value for key, value in required_counts.items()):
        raise ReviewedReleaseStagingError(
            "evidence release counts do not describe exactly one candidate slice"
        )
    if (
        expected.get("machine_generated") is not True
        or expected.get("unreviewed") is not True
        or expected.get("verified_quotation") is not False
        or expected.get("disclaimer_code") != DISCLAIMER_CODE
        or revision.get("required_public_warning") != DISCLAIMER_TEXT
    ):
        raise ReviewedReleaseStagingError(
            "evidence does not preserve the mandatory unreviewed-machine warning"
        )
    speaker = revision.get("speaker_labels")
    if not isinstance(speaker, dict) or speaker.get("named_segment_count") != 0:
        raise ReviewedReleaseStagingError(
            "candidate must remain an unnamed-speaker machine transcript"
        )
    return {
        "source_id": source_id,
        "recording_id": recording_id,
        "revision_id": revision_id,
        "segment_count": segment_count,
        "counts": required_counts,
    }


def _load_completed_manifests(
    reviewer_path: Path, publication_path: Path, expectation: dict[str, Any]
) -> tuple[ReviewerAdminManifest, PublicationManifest, bytes, bytes]:
    reviewer_body = _stable_read(reviewer_path, "reviewer manifest")
    publication_body = _stable_read(publication_path, "publication manifest")
    _reject_placeholders(
        _json_without_duplicate_keys(reviewer_body, "reviewer manifest")
    )
    _reject_placeholders(
        _json_without_duplicate_keys(publication_body, "publication manifest")
    )
    with tempfile.TemporaryDirectory(prefix="reviewed-release-inputs-") as directory:
        input_root = Path(directory)
        input_root.chmod(0o700)
        reviewer_copy = input_root / "reviewer.json"
        publication_copy = input_root / "publication.json"
        _write_private(reviewer_copy, reviewer_body, sealed=True)
        _write_private(publication_copy, publication_body, sealed=True)
        reviewer = load_reviewer_admin_manifest(reviewer_copy)
        publication = load_publication_manifest(publication_copy)

    if len(reviewer.registrations) != 1 or len(reviewer.state_changes) != 1:
        raise ReviewedReleaseStagingError(
            "reviewer manifest must register and activate exactly one reviewer"
        )
    registration = reviewer.registrations[0]
    state = reviewer.state_changes[0]
    if (
        registration.reviewer_kind != "human"
        or state.reviewer_id != registration.reviewer_id
        or state.active is not True
        or state.changed_at_value <= registration.registered_at_value
    ):
        raise ReviewedReleaseStagingError(
            "reviewer manifest must explicitly activate its one human reviewer later"
        )
    reviewer_id = registration.reviewer_id

    expected_publications = {
        ("source", expectation["source_id"]),
        ("recording", expectation["recording_id"]),
    }
    actual_publications = {
        (decision.object_type, decision.object_id)
        for decision in publication.publication_decisions
    }
    if (
        len(publication.publication_decisions) != 2
        or actual_publications != expected_publications
        or any(
            decision.decision != "publish" or decision.reviewer_id != reviewer_id
            for decision in publication.publication_decisions
        )
    ):
        raise ReviewedReleaseStagingError(
            "publication manifest must contain exactly the candidate source and "
            "recording human publish decisions"
        )

    expected_gates = {
        (object_type, object_id, gate_kind)
        for object_type, object_id in (
            ("source", expectation["source_id"]),
            ("recording", expectation["recording_id"]),
            ("transcript_revision", expectation["revision_id"]),
        )
        for gate_kind in ("rights", "privacy", "sensitivity")
    }
    actual_gates = {
        (decision.object_type, decision.object_id, decision.gate_kind)
        for decision in publication.gate_decisions
    }
    if (
        len(publication.gate_decisions) != 9
        or actual_gates != expected_gates
        or any(
            decision.decision != "clear" or decision.reviewer_id != reviewer_id
            for decision in publication.gate_decisions
        )
    ):
        raise ReviewedReleaseStagingError(
            "publication manifest must contain exactly nine independent human "
            "clear decisions: rights, privacy, and sensitivity for the candidate "
            "source, recording, and transcript revision"
        )
    return reviewer, publication, reviewer_body, publication_body


def _is_within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def _private_work_root(path: Path) -> Path:
    if not path.is_absolute():
        raise ReviewedReleaseStagingError("work root must be an absolute path")
    prospective = path.resolve(strict=False)
    if not any(
        _is_within(prospective, allowed.resolve()) for allowed in ALLOWED_OUTPUT_ROOTS
    ):
        raise ReviewedReleaseStagingError(
            "work root must be below the private research/corpus tree or /tmp"
        )
    if any(_is_within(prospective, root.resolve()) for root in FORBIDDEN_OUTPUT_ROOTS):
        raise ReviewedReleaseStagingError(
            "work root must not be inside a source, public, dist, or Git tree"
        )
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    supplied = path.lstat()
    if stat.S_ISLNK(supplied.st_mode) or not stat.S_ISDIR(supplied.st_mode):
        raise ReviewedReleaseStagingError(
            "work root must be a non-symlink directory"
        )
    resolved = path.resolve(strict=True)
    info = resolved.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ReviewedReleaseStagingError(
            "work root must be a non-symlink directory"
        )
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ReviewedReleaseStagingError(
            "work root must be owned by the current user and inaccessible to group/other"
        )
    return resolved


def _copy_sealed_catalog(
    base: Path, destination: Path, expected_sha256: str
) -> tuple[str, int]:
    expected_sha256 = _required_sha256(expected_sha256, "base catalog SHA-256")
    before_path = _regular_nonsymlink(base, "base catalog")
    base = base.resolve(strict=True)
    if stat.S_IMODE(before_path.st_mode) & 0o222:
        raise ReviewedReleaseStagingError("base catalog must be sealed read-only")
    sidecars = [
        Path(f"{base}{suffix}")
        for suffix in ("-wal", "-shm", "-journal")
        if Path(f"{base}{suffix}").exists()
    ]
    if sidecars:
        raise ReviewedReleaseStagingError(
            "base catalog must have no SQLite sidecars; found "
            + ", ".join(item.name for item in sidecars)
        )
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
    temporary = destination.with_name(f".{destination.name}.copying")
    try:
        with base.open("rb") as source, temporary.open("xb") as target:
            descriptor_before = os.fstat(source.fileno())
            if _identity(descriptor_before) != _identity(before_path):
                raise ReviewedReleaseStagingError(
                    "base catalog was replaced while being opened"
                )
            observed, byte_count = _sha256_stream(source, copy_to=target)
            target.flush()
            os.fsync(target.fileno())
            descriptor_after = os.fstat(source.fileno())
        if (
            _identity(descriptor_before) != _identity(descriptor_after)
            or _identity(descriptor_before) != _identity(base.stat())
        ):
            raise ReviewedReleaseStagingError(
                "base catalog changed while being copied"
            )
        if observed != expected_sha256:
            raise ReviewedReleaseStagingError(
                f"base catalog SHA-256 mismatch: expected {expected_sha256}, "
                f"observed {observed}"
            )
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    return observed, byte_count


def _write_private(path: Path, body: bytes, *, sealed: bool = False) -> str:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    path.chmod(0o400 if sealed else 0o600)
    return hashlib.sha256(body).hexdigest()


def _close_staging_catalog(connection: Any, catalog_path: Path) -> None:
    try:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("PRAGMA journal_mode = DELETE")
    finally:
        connection.close()
    sidecars = [
        Path(f"{catalog_path}{suffix}")
        for suffix in ("-wal", "-shm", "-journal")
        if Path(f"{catalog_path}{suffix}").exists()
    ]
    if sidecars:
        raise ReviewedReleaseStagingError(
            "staging catalog did not close without sidecars: "
            + ", ".join(item.name for item in sidecars)
        )


def _seal_tree(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            raise ReviewedReleaseStagingError("staged output contains a symlink")
        path.chmod(0o500 if path.is_dir() else 0o400)
    root.chmod(0o500)


def _assert_policy_plan(plan: dict[str, Any], expectation: dict[str, Any]) -> None:
    revisions = plan.get("revisions")
    if (
        plan.get("eligible_revision_count") != 1
        or not isinstance(revisions, list)
        or len(revisions) != 1
        or revisions[0].get("revision_id") != expectation["revision_id"]
        or revisions[0].get("recording_id") != expectation["recording_id"]
        or revisions[0].get("segment_count") != expectation["segment_count"]
        or plan.get("disclaimer_code") != DISCLAIMER_CODE
        or plan.get("disclaimer") != DISCLAIMER_TEXT
    ):
        raise ReviewedReleaseStagingError(
            "text-free machine publication plan is not exactly the reviewed candidate"
        )


def _assert_release(release: dict[str, Any], expectation: dict[str, Any]) -> None:
    if release.get("counts") != expectation["counts"]:
        raise ReviewedReleaseStagingError(
            f"release counts differ from the packet: {release.get('counts')!r}"
        )
    recordings = release.get("recordings")
    if not isinstance(recordings, list) or len(recordings) != 1:
        raise ReviewedReleaseStagingError("release must contain exactly one recording")
    recording = recordings[0]
    if recording.get("recording_id") != expectation["recording_id"]:
        raise ReviewedReleaseStagingError("release recording differs from the packet")
    if [item.get("source_id") for item in recording.get("sources", [])] != [
        expectation["source_id"]
    ]:
        raise ReviewedReleaseStagingError("release source differs from the packet")
    revisions = recording.get("transcript_revisions")
    if not isinstance(revisions, list) or len(revisions) != 1:
        raise ReviewedReleaseStagingError("release must contain exactly one revision")
    revision = revisions[0]
    if (
        revision.get("revision_id") != expectation["revision_id"]
        or revision.get("machine_generated") is not True
        or revision.get("unreviewed") is not True
        or revision.get("verified_quotation") is not False
        or revision.get("disclaimer_code") != DISCLAIMER_CODE
        or len(revision.get("segments", [])) != expectation["segment_count"]
        or any(
            segment.get("speaker_label") is not None
            for segment in revision.get("segments", [])
        )
    ):
        raise ReviewedReleaseStagingError(
            "release does not preserve the exact unnamed, unreviewed machine slice"
        )


def stage_reviewed_release(
    *,
    mode: str,
    base_catalog: Path,
    expected_base_sha256: str,
    evidence_path: Path,
    expected_evidence_sha256: str,
    reviewer_manifest_path: Path,
    publication_manifest_path: Path,
    work_root: Path,
    expected_machine_plan_sha256: str | None = None,
    expected_reviewer_manifest_sha256: str | None = None,
    expected_publication_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Create a private plan rehearsal or a private candidate release.

    ``plan`` stops before the closed machine publication decision is applied.
    ``build`` repeats the complete rehearsal and requires the reviewed plan and both
    manifest digests before applying that decision and exporting private shards.
    """

    if mode not in {"plan", "build"}:
        raise ReviewedReleaseStagingError("mode must be 'plan' or 'build'")
    evidence, evidence_body = _load_pinned_json(
        evidence_path, expected_evidence_sha256, "packet evidence"
    )
    expectation = _packet_expectation(evidence)
    reviewer, publication, reviewer_body, publication_body = _load_completed_manifests(
        reviewer_manifest_path, publication_manifest_path, expectation
    )
    reviewer_sha256 = hashlib.sha256(
        _canonical_json(
            _json_without_duplicate_keys(reviewer_body, "reviewer manifest")
        )
    ).hexdigest()
    publication_sha256 = hashlib.sha256(
        _canonical_json(
            _json_without_duplicate_keys(publication_body, "publication manifest")
        )
    ).hexdigest()
    if (
        reviewer_sha256 != reviewer.input_sha256
        or publication_sha256 != publication.input_sha256
    ):
        raise ReviewedReleaseStagingError("manifest canonical digest disagreement")
    if mode == "build":
        if expected_machine_plan_sha256 is None:
            raise ReviewedReleaseStagingError(
                "build requires --expected-machine-plan-sha256 from a reviewed plan"
            )
        _required_sha256(expected_machine_plan_sha256, "machine plan SHA-256")
        for observed, expected, label in (
            (
                reviewer_sha256,
                expected_reviewer_manifest_sha256,
                "reviewer manifest",
            ),
            (
                publication_sha256,
                expected_publication_manifest_sha256,
                "publication manifest",
            ),
        ):
            if expected is None:
                raise ReviewedReleaseStagingError(
                    f"build requires the reviewed {label} SHA-256"
                )
            _required_sha256(expected, f"expected {label} SHA-256")
            if observed != expected:
                raise ReviewedReleaseStagingError(
                    f"{label} changed after the plan rehearsal"
                )

    private_root = _private_work_root(work_root)
    run_root = Path(
        tempfile.mkdtemp(prefix=f"reviewed-release-{mode}-", dir=private_root)
    )
    run_root.chmod(0o700)
    catalog_path = run_root / "catalog" / "catalog.sqlite3"
    release_root = run_root / "candidate-release"
    try:
        base_sha256, base_bytes = _copy_sealed_catalog(
            base_catalog, catalog_path, expected_base_sha256
        )
        input_root = run_root / "inputs"
        input_root.mkdir(mode=0o700)
        reviewer_copy = input_root / "reviewer-admin.json"
        publication_copy = input_root / "publication-manifest.json"
        _write_private(reviewer_copy, reviewer_body, sealed=True)
        _write_private(publication_copy, publication_body, sealed=True)
        _write_private(input_root / "evidence.json", evidence_body, sealed=True)

        connection = connect(catalog_path)
        closed = False
        try:
            applied_migrations = migrate(connection)
            reviewer_result = apply_reviewer_admin_manifest(
                connection, reviewer_copy, dry_run=False
            )
            publication_result = apply_publication_manifest(
                connection, publication_copy, dry_run=False
            )
            policy_plan = build_machine_transcript_publication_plan(connection)
            _assert_policy_plan(policy_plan, expectation)
            policy_result: dict[str, Any] | None = None
            release_result: dict[str, Any] | None = None
            release_validation: dict[str, Any] | None = None
            if mode == "build":
                if policy_plan["plan_sha256"] != expected_machine_plan_sha256:
                    raise ReviewedReleaseStagingError(
                        "machine publication plan changed after human digest review"
                    )
                policy_result = apply_machine_transcript_publication_plan(
                    connection,
                    expected_plan_sha256=expected_machine_plan_sha256,
                )
                database_validation = validate_database(connection)
                release = build_release(connection)
                _assert_release(release, expectation)
                release_result = export_release_v2_from_release(release, release_root)
                release_validation = validate_sharded_release(
                    release_root / "manifest.json"
                )
            else:
                database_validation = validate_database(connection)
            _close_staging_catalog(connection, catalog_path)
            closed = True
        finally:
            if not closed:
                connection.close()

        receipt = {
            "schema_version": SCHEMA_VERSION,
            "state": "candidate_not_published" if mode == "build" else "plan_only",
            "mode": mode,
            "live_catalog_opened": False,
            "live_catalog_mutated": False,
            "public_tree_written": False,
            "wording_review_performed": False,
            "identity_claimed": False,
            "base_catalog": {
                "sha256": base_sha256,
                "byte_count": base_bytes,
            },
            "packet_evidence_sha256": expected_evidence_sha256,
            "reviewer_manifest_sha256": reviewer_sha256,
            "publication_manifest_sha256": publication_sha256,
            "candidate": expectation,
            "applied_migrations": applied_migrations,
            "reviewer_result": reviewer_result,
            "publication_result": publication_result,
            "machine_publication_plan": policy_plan,
            "machine_publication_result": policy_result,
            "database_validation": database_validation,
            "release_result": release_result,
            "release_validation": release_validation,
            "mandatory_disclaimer": DISCLAIMER_TEXT,
        }
        receipt_path = run_root / "receipt.json"
        receipt_bytes = json.dumps(
            receipt, ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8") + b"\n"
        receipt_sha256 = _write_private(receipt_path, receipt_bytes, sealed=True)
        catalog_path.chmod(0o400)
        if release_root.exists():
            _seal_tree(release_root)
        return {
            "state": receipt["state"],
            "run_directory": str(run_root),
            "receipt_path": str(receipt_path),
            "receipt_sha256": receipt_sha256,
            "machine_plan_sha256": policy_plan["plan_sha256"],
            "reviewer_manifest_sha256": reviewer_sha256,
            "publication_manifest_sha256": publication_sha256,
            "candidate_release_manifest": (
                str(release_root / "manifest.json") if release_root.exists() else None
            ),
        }
    except Exception:
        # Keep a failed private rehearsal for diagnosis, but never leave it broadly
        # readable and never mistake it for a completed receipt.
        for path in run_root.rglob("*"):
            try:
                if path.is_dir():
                    path.chmod(0o700)
                elif not path.is_symlink():
                    path.chmod(0o600)
            except OSError:
                pass
        run_root.chmod(0o700)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="himr-reviewed-release-staging")
    subparsers = parser.add_subparsers(dest="mode", required=True)
    for mode in ("plan", "build"):
        command = subparsers.add_parser(mode)
        command.add_argument("--base-catalog", type=Path, required=True)
        command.add_argument("--expected-base-sha256", required=True)
        command.add_argument("--evidence", type=Path, required=True)
        command.add_argument("--expected-evidence-sha256", required=True)
        command.add_argument("--reviewer-manifest", type=Path, required=True)
        command.add_argument("--publication-manifest", type=Path, required=True)
        command.add_argument("--work-root", type=Path, required=True)
    build = subparsers.choices["build"]
    build.add_argument("--expected-machine-plan-sha256", required=True)
    build.add_argument("--expected-reviewer-manifest-sha256", required=True)
    build.add_argument("--expected-publication-manifest-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = stage_reviewed_release(
            mode=args.mode,
            base_catalog=args.base_catalog,
            expected_base_sha256=args.expected_base_sha256,
            evidence_path=args.evidence,
            expected_evidence_sha256=args.expected_evidence_sha256,
            reviewer_manifest_path=args.reviewer_manifest,
            publication_manifest_path=args.publication_manifest,
            work_root=args.work_root,
            expected_machine_plan_sha256=getattr(
                args, "expected_machine_plan_sha256", None
            ),
            expected_reviewer_manifest_sha256=getattr(
                args, "expected_reviewer_manifest_sha256", None
            ),
            expected_publication_manifest_sha256=getattr(
                args, "expected_publication_manifest_sha256", None
            ),
        )
    except (
        ReviewedReleaseStagingError,
        MachineTranscriptPublicationPolicyError,
        PublicationManifestError,
        ReviewerAdminManifestError,
    ) as error:
        parser.exit(1, f"staging refused: {error}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
