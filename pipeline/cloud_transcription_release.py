"""Explicit same-workspace code release for future cloud title decisions only.

This is not a plan migration, recovery admission, or a replacement validator.
The caller must supply an externally retained release SHA-256. Original plans,
workspace markers, paid intents, receipts and spending limits remain unchanged.
Old source snapshots are authenticated as bytes and are never executed. Runtime
loaders retain every non-implementation check, including original marker hashes.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
import hashlib
import os
from pathlib import Path

from pipeline import transcript_summary as io

KIND = "himr_cloud_transcription_execution_release"
SNAPSHOT_KIND = "himr_cloud_execution_code_snapshot"
MAX_RELEASE_BYTES = 256 * 1024
MAX_CODE_FILES = 64
CHANGED_EXISTING = frozenset({"cloud_transcription.py", "cloud_transcription_summary.py"})
ADDED_MODULES = frozenset({"cloud_transcription_release.py", "cloud_transcription_title_policy.py"})
SEMANTICS = {
    "scope": "future_unsubmitted_title_diarization_decisions_only",
    "original_manifests_unchanged": True,
    "original_workspace_markers_unchanged": True,
    "original_paid_evidence_unchanged": True,
    "spending_limits_unchanged": True,
    "no_automatic_retranscription": True,
    "existing_transcripts_title_review_only": True,
    "source_mutation": False,
    "old_code_execution": False,
    "new_paid_requests_during_preparation": 0,
}
_ACTIVE = ContextVar("himr_cloud_transcription_execution_release", default=None)


class ReleaseError(RuntimeError):
    pass


def _mapping(value, label):
    if not isinstance(value, dict) or not 1 <= len(value) <= MAX_CODE_FILES:
        raise ReleaseError(label + " implementation map exceeds its bound")
    for name, digest in value.items():
        if (not isinstance(name, str) or Path(name).name != name or not name.endswith(".py")
                or not isinstance(digest, str) or not io.safe.SHA.fullmatch(digest)):
            raise ReleaseError(label + " has an invalid source identity")
    return value


def _union(left, right):
    for name in left.keys() & right.keys():
        if left[name] != right[name]:
            raise ReleaseError("cloud and summary source identities disagree")
    return {**left, **right}


def _current():
    # These imports must not call either runtime loader: they may be validating
    # a release from inside those loaders' implementation checks.
    from pipeline import cloud_transcription as cloud
    from pipeline import cloud_transcription_summary as summaries
    folder = Path(__file__).parent
    package_init = folder / "__init__.py"
    return {
        "cloud": cloud.implementation(),
        "summary": summaries.implementation(),
        "parallel_sha256": hashlib.sha256((folder / "cloud_transcription_parallel.py").read_bytes()).hexdigest(),
        "release_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "package_init_sha256": hashlib.sha256(package_init.read_bytes()).hexdigest() if package_init.exists() else None,
    }


def _originals(cloud_plan_ref, summary_manifest_ref):
    from pipeline import cloud_transcription as cloud
    from pipeline import cloud_transcription_summary as summaries
    plan, manifest = io.read(cloud_plan_ref), io.read(summary_manifest_ref)
    if (plan.get("kind") != cloud.KIND or type(plan.get("schema_version")) is not int
            or plan["schema_version"] != 1 or plan.get("policy") != cloud.POLICY
            or plan.get("rates_microusd_hour") != cloud.RATES
            or manifest.get("kind") != summaries.KIND or type(manifest.get("schema_version")) is not int
            or manifest["schema_version"] != 1 or manifest.get("policy") != summaries.POLICY
            or manifest.get("cloud_plan") != cloud_plan_ref):
        raise ReleaseError("release requires the unchanged bound cloud and summary contracts")
    for reference, value, filename in ((cloud_plan_ref, plan, "plan.json"),
                                       (summary_manifest_ref, manifest, "manifest.json")):
        root = io.safe.path_value(value.get("state_root"))
        if Path(reference["path"]) != root / filename:
            raise ReleaseError("original manifest escaped its private workspace")
        _mapping(value.get("implementation"), "original")
    io.safe.integer(manifest.get("max_total_budget_microusd"), 1, 10_000_000_000, "original summary budget")
    _union(plan["implementation"], manifest["implementation"])
    return plan, manifest


def _snapshot(reference, cloud_plan_ref, summary_manifest_ref, plan, manifest):
    snapshot = io.read(reference)
    io.safe.exact(snapshot, {"kind", "schema_version", "cloud_plan", "summary_manifest", "implementation",
                             "files", "new_paid_requests", "source_mutation"}, "old execution source snapshot")
    expected = _union(plan["implementation"], manifest["implementation"])
    if (snapshot["kind"] != SNAPSHOT_KIND or type(snapshot["schema_version"]) is not int
            or snapshot["schema_version"] != 1 or snapshot["cloud_plan"] != cloud_plan_ref
            or snapshot["summary_manifest"] != summary_manifest_ref or snapshot["implementation"] != expected
            or type(snapshot["new_paid_requests"]) is not int or snapshot["new_paid_requests"] != 0
            or snapshot["source_mutation"] is not False or not isinstance(snapshot["files"], dict)
            or set(snapshot["files"]) != set(expected)):
        raise ReleaseError("old execution source snapshot does not exactly prove both original manifests")
    for name, proof in snapshot["files"].items():
        io.safe.file_binding(proof)
        if Path(proof["path"]).name != name or proof["sha256"] != expected[name]:
            raise ReleaseError("old execution source filename or digest differs")
        io.read_bytes(proof)
    return snapshot


def _approved_delta(original, current):
    _mapping(current, "current")
    if set(original) - set(current) or set(current) - set(original) - ADDED_MODULES:
        raise ReleaseError("execution release source set exceeds the approved title-policy change")
    changed = {name for name in original if original[name] != current[name]}
    if changed - CHANGED_EXISTING:
        raise ReleaseError("execution release changed unrelated or evidence-validation source code")


def _policy(reference):
    from pipeline import cloud_transcription_title_policy as titles
    return titles.load_policy(reference)


def _validate(value):
    io.safe.exact(value, {"kind", "schema_version", "cloud_plan", "summary_manifest", "old_code",
                         "old_cloud_implementation", "old_summary_implementation", "implementation",
                         "title_policy", "summary_budget_microusd", "semantics"}, "execution release")
    if (value["kind"] != KIND or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["semantics"] != SEMANTICS):
        raise ReleaseError("execution release scope or version differs")
    plan, manifest = _originals(value["cloud_plan"], value["summary_manifest"])
    if (value["old_cloud_implementation"] != plan["implementation"]
            or value["old_summary_implementation"] != manifest["implementation"]
            or value["summary_budget_microusd"] != manifest["max_total_budget_microusd"]):
        raise ReleaseError("execution release changed original implementation or budget authority")
    _snapshot(value["old_code"], value["cloud_plan"], value["summary_manifest"], plan, manifest)
    expected = _current()
    if value["implementation"] != expected:
        raise ReleaseError("execution release current code changed; explicit new release required")
    _union(expected["cloud"], expected["summary"])
    _approved_delta(plan["implementation"], expected["cloud"])
    _approved_delta(manifest["implementation"], expected["summary"])
    _policy(value["title_policy"])
    return value


def load(reference):
    """Hash-validate all release/code proofs; never execute old snapshots."""
    raw = io.read_bytes(reference)
    if len(raw) > MAX_RELEASE_BYTES:
        raise ReleaseError("execution release exceeds its metadata bound")
    return _validate(io.parse(raw))


def snapshot_code(cloud_plan_ref, summary_manifest_ref, output_root):
    """Before editing, retain the exact live implementation union as inert bytes.

    The operator owns graceful worker shutdown; this helper neither controls
    processes nor changes a runtime. A partial interrupted copy never publishes
    the final snapshot proof and cannot authorize a release.
    """
    plan, manifest = _originals(cloud_plan_ref, summary_manifest_ref)
    before = _current()
    if before["cloud"] != plan["implementation"] or before["summary"] != manifest["implementation"]:
        raise ReleaseError("snapshot requires the exact original implementation before any code edits")
    original = _union(plan["implementation"], manifest["implementation"])
    root = io.safe.path_value(output_root)
    io.protect(root, {"cloud_plan": cloud_plan_ref, "summary_manifest": summary_manifest_ref,
                      "cloud_root": plan["state_root"], "summary_root": manifest["state_root"]})
    if io.safe.exists(root):
        with io.paths.retained_directory(root) as directory:
            with os.scandir(directory) as entries:
                if any(entries):
                    raise ReleaseError("code snapshot requires a fresh or empty private output root")
    folder = Path(__file__).parent
    source_bytes = {name: io.read_bytes({"path": str(folder / name), "sha256": digest})
                    for name, digest in original.items()}
    io.mkdir(root)
    io.mkdir(root / "old-code")
    files = {name: io.put_bytes(root / "old-code" / name, raw) for name, raw in source_bytes.items()}
    if _current() != before:
        raise ReleaseError("implementation changed during snapshot; no snapshot proof published")
    snapshot = {"kind": SNAPSHOT_KIND, "schema_version": 1, "cloud_plan": cloud_plan_ref,
                "summary_manifest": summary_manifest_ref, "implementation": original, "files": files,
                "new_paid_requests": 0, "source_mutation": False}
    reference = io.put(root / "old-code.json", snapshot)
    _snapshot(reference, cloud_plan_ref, summary_manifest_ref, plan, manifest)
    return reference


def prepare_release(cloud_plan_ref, summary_manifest_ref, *, old_code_ref, title_policy_ref, output):
    """Publish a fresh immutable release outside both original workspaces."""
    plan, manifest = _originals(cloud_plan_ref, summary_manifest_ref)
    value = {"kind": KIND, "schema_version": 1, "cloud_plan": cloud_plan_ref,
             "summary_manifest": summary_manifest_ref, "old_code": old_code_ref,
             "old_cloud_implementation": plan["implementation"],
             "old_summary_implementation": manifest["implementation"], "implementation": _current(),
             "title_policy": title_policy_ref, "summary_budget_microusd": manifest["max_total_budget_microusd"],
             "semantics": SEMANTICS}
    _validate(value)
    output = io.safe.path_value(output)
    io.protect(output.parent, {"release": value, "cloud_root": plan["state_root"],
                              "summary_root": manifest["state_root"]})
    if io.safe.exists(output):
        raise ReleaseError("execution release output must be fresh")
    raw = io.canonical(value)
    if len(raw) > MAX_RELEASE_BYTES:
        raise ReleaseError("execution release exceeds its metadata bound")
    io.mkdir(output.parent)
    return {"state": "execution_release_prepared_offline", "execution_release": io.put_bytes(output, raw),
            "new_paid_requests": 0, "original_artifacts_modified": False}


@contextmanager
def activate(reference):
    """Opt in for this context only; no global/environment/filename discovery."""
    if _ACTIVE.get() is not None:
        raise ReleaseError("nested execution release activation is not permitted")
    value = load(reference)
    token = _ACTIVE.set((deepcopy(reference), value))
    try:
        yield deepcopy(reference)
    finally:
        _ACTIVE.reset(token)


def _context():
    active = _ACTIVE.get()
    if active is not None:
        # A modified/deleted release cannot silently remain an active exception.
        # Code hashes are supplied afresh by the original runtime loaders.
        if io.read(active[0]) != active[1]:
            raise ReleaseError("active execution release changed")
    return active


def _permits(reference, original, current, name):
    active = _context()
    if active is None:
        return False
    value = active[1]
    ref_key = "cloud_plan" if name == "cloud" else "summary_manifest"
    return (reference == value[ref_key] and original == value["old_" + name + "_implementation"]
            and current == value["implementation"][name])


def permits_cloud(reference, original, current):
    return _permits(reference, original, current, "cloud")


def permits_summary(reference, original, current):
    return _permits(reference, original, current, "summary")


def active_title_policy(cloud_plan_ref):
    active = _context()
    if active is None or cloud_plan_ref != active[1]["cloud_plan"]:
        return None
    # Recheck the exact policy bytes, not a mutable caller-owned object.
    _policy(active[1]["title_policy"])
    return deepcopy(active[1]["title_policy"])
