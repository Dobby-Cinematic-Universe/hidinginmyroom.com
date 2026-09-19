"""Hash-bound, offline recovery checkpoints for the unchanged Gemini runner.

This optional launcher primes only the runner's existing bounded admission
caches. It never replaces validators, changes implementation checks, imports
unbound results, or trusts cached live submission state or budget totals. The caller
must retain the checkpoint's exact SHA-256 outside the checkpoint itself.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import stat
import sys
import time

from pipeline import transcript_summary as r
from pipeline import transcript_summary_campaign as campaign
from pipeline import transcript_summary_classification as classification
from pipeline import transcript_summary_recovery as recovery

KIND = "himr_gemini_recovery_checkpoint"
MAX_CHECKPOINT_BYTES = 4 * 1024**2
MAX_ENTRIES = 65
MAX_DIRECTORIES = 8192
MAX_NAMES = 100000


def implementation():
    return {**campaign.implementation(), Path(__file__).name:
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def clear_caches():
    """Affect this launcher process only; never touch a running service."""
    recovery._CACHE.clear()
    recovery._CACHE_BYTES = 0
    classification._CACHE.clear()


def lineage(manifest):
    """Find closed producer roots and admission bindings, not live shard state."""
    catalog, roots, seen, pending = {}, set(), set(), [manifest]
    while pending:
        value = pending.pop()
        prior = value.get("recovery", {})
        for ref in prior.get("imports", {}).values():
            r.safe.file_binding(ref)
            catalog.setdefault(ref["sha256"], ref)
        refs = [prior["producer_manifest"]] if "producer_manifest" in prior else []
        refs += [prior[key]["producer_manifest"] for key in ("schema_repair", "continuation") if key in prior]
        for ref in refs:
            identity = (ref["path"], ref["sha256"])
            if identity in seen:
                continue
            seen.add(identity)
            if len(seen) > 8:
                raise r.Error("checkpoint producer lineage exceeds bound")
            producer = r.read(ref)
            root = str(r.safe.path_value(producer["state_root"]))
            if root == manifest["state_root"]:
                raise r.Error("checkpoint cannot cache the active producer workspace")
            roots.add(root)
            pending.append(producer)
    return catalog, sorted(roots)


def inventories(roots):
    """Bind absence as well as presence of old shards and paid-wave files.

    A new old-producer wave/receipt must invalidate the cache even when all
    previously hashed files are unchanged. Live campaign files are excluded.
    """
    records, names = [], 0

    def listing(folder, *, shards_only=False):
        nonlocal names
        entries = []
        with r.paths.retained_directory(folder) as directory:
            with os.scandir(directory) as found:
                for entry in found:
                    name = entry.name
                    if shards_only and not name.startswith("shard-"):
                        continue
                    if name.endswith(".lock") or name == "status.json" or name.startswith(".summary-"):
                        continue
                    info = entry.stat(follow_symlinks=False)
                    kind = "directory" if stat.S_ISDIR(info.st_mode) else "file" if stat.S_ISREG(info.st_mode) else None
                    if kind is None:
                        raise r.Error("unsafe producer inventory entry")
                    entries.append([name, kind])
        entries.sort()
        names += len(entries)
        records.append({"path": str(folder), "entries": entries})
        if len(records) > MAX_DIRECTORIES or names > MAX_NAMES:
            raise r.Error("checkpoint producer inventory exceeds bound")
        return entries

    for path in roots:
        root = Path(path)
        for name, kind in listing(root, shards_only=True):
            if kind != "directory":
                raise r.Error("producer shard is not a directory")
            folder = root / name
            entries = listing(folder)
            if ["waves", "directory"] not in entries:
                continue
            for wave, kind in listing(folder / "waves"):
                if kind == "directory":
                    listing(folder / "waves" / wave)
    return records


def _read_checkpoint(ref):
    data = r.read_bytes(ref)
    if len(data) > MAX_CHECKPOINT_BYTES:
        raise r.Error("recovery checkpoint exceeds metadata bound")
    value = r.parse(data)
    r.safe.exact(value, {"kind", "schema_version", "manifest", "implementation", "entries",
                        "producer_inventory", "validation", "prepare_seconds"}, "recovery checkpoint")
    if (value["kind"] != KIND or type(value["schema_version"]) is not int or value["schema_version"] != 1 or
            value["implementation"] != implementation()):
        raise r.Error("checkpoint implementation or version changed; rebuild offline")
    r.safe.file_binding(value["manifest"])
    if not isinstance(value["entries"], list) or not 1 <= len(value["entries"]) <= MAX_ENTRIES:
        raise r.Error("invalid checkpoint cache entry count")
    if not isinstance(value["producer_inventory"], list) or len(value["producer_inventory"]) > MAX_DIRECTORIES:
        raise r.Error("invalid checkpoint producer inventory")
    return value


def prepare(manifest_ref, output, *, progress=None):
    """One cold, full replay. Publish an immutable attestation, not copied results."""
    started = time.monotonic()
    manifest = campaign.validate_manifest(r.read(manifest_ref))
    if not manifest.get("recovery", {}).get("imports"):
        raise r.Error("recovery checkpoint requires imported paid results")
    output = r.safe.path_value(output)
    r.protect(output.parent, {"manifest": manifest_ref, "campaign": manifest})
    if r.safe.exists(output):
        raise r.Error("checkpoint output must be fresh")
    code = implementation()
    catalog, roots = lineage(manifest)
    before = inventories(roots)
    clear_caches()
    validation = recovery.validate_campaign(manifest, progress=progress)
    entries = []
    stores = [(recovery.KIND, recovery._CACHE), (classification.KIND, classification._CACHE)]
    for kind, cache in stores:
        for key, cached in cache.items():
            witnesses, admission = cached[:2]
            ref = catalog.get(key[0])
            if ref is None or r.read(ref) != admission or admission["kind"] != kind:
                raise r.Error("validated cache entry is outside the bound campaign lineage")
            if witnesses != [recovery._witness(proof) for proof in admission["proofs"]]:
                raise r.Error("producer proof changed during checkpoint preparation")
            entries.append({"admission": ref, "sources_sha256": key[1], "config_sha256": key[2],
                            "kind": kind, "proof_witnesses": witnesses})
    if not 1 <= len(entries) <= MAX_ENTRIES or before != inventories(roots) or code != implementation():
        raise r.Error("producer inventory or implementation changed during checkpoint preparation")
    value = {"kind": KIND, "schema_version": 1, "manifest": manifest_ref, "implementation": code,
             "entries": entries, "producer_inventory": before, "validation": validation,
             "prepare_seconds": time.monotonic() - started}
    if len(r.canonical(value)) > MAX_CHECKPOINT_BYTES:
        raise r.Error("recovery checkpoint exceeds metadata bound")
    r.mkdir(output.parent)
    return {"state": "checkpoint_prepared_offline", "checkpoint": r.put(output, value),
            "cached_admissions": len(entries), "prepare_seconds": value["prepare_seconds"], "new_paid_requests": 0}


def restore(checkpoint_ref):
    """Restore only attested, unchanged entries; changed entries replay normally."""
    started = time.monotonic()
    value = _read_checkpoint(checkpoint_ref)
    manifest = campaign.validate_manifest(r.read(value["manifest"]))
    catalog, roots = lineage(manifest)
    same_inventory = inventories(roots) == value["producer_inventory"]
    clear_caches()
    prepared, keys, total, continuation_count = [], set(), 0, 0
    for entry in value["entries"]:
        r.safe.exact(entry, {"admission", "sources_sha256", "config_sha256", "kind", "proof_witnesses"}, "checkpoint cache entry")
        ref = entry["admission"]
        r.safe.file_binding(ref)
        for name in ("sources_sha256", "config_sha256"):
            r.safe.file_binding({"path": ref["path"], "sha256": entry[name]})
        key = (ref["sha256"], entry["sources_sha256"], entry["config_sha256"])
        if key in keys or catalog.get(key[0]) != ref:
            raise r.Error("checkpoint entry is duplicate or outside its campaign lineage")
        keys.add(key)
        admission = r.read(ref)  # Always verify the exact original result bytes.
        if (entry["kind"] not in {recovery.KIND, classification.KIND} or admission["kind"] != entry["kind"] or
                r.digest(admission["config"]) != entry["config_sha256"] or
                len(admission["proofs"]) > 20000 or not isinstance(entry["proof_witnesses"], list) or
                len(entry["proof_witnesses"]) != len(admission["proofs"])):
            raise r.Error("checkpoint admission contract differs")
        current = [recovery._witness(proof) for proof in admission["proofs"]]
        if not same_inventory or current != entry["proof_witnesses"]:
            continue  # The unchanged original validator will perform a cold replay.
        size = len(r.canonical(admission))
        if entry["kind"] == recovery.KIND:
            total += size
            if total > recovery.MAX_CACHE_BYTES or sum(kind == recovery.KIND for kind, *_ in prepared) >= 64:
                raise r.Error("checkpoint exceeds the original admission cache bound")
        else:
            continuation_count += 1
            if continuation_count > 1 or size > 128 * 1024**2:
                raise r.Error("checkpoint exceeds the original continuation cache bound")
        prepared.append((entry["kind"], key, current, admission, size))
    # No partial cache restoration if any checkpoint entry is malformed.
    for kind, key, witnesses, admission, size in prepared:
        if kind == recovery.KIND:
            recovery._CACHE[key] = (witnesses, admission, size)
        else:
            classification._CACHE[key] = (witnesses, admission)
    recovery._CACHE_BYTES = total
    return value["manifest"], {"state": "checkpoint_restored", "restored_admissions": len(prepared),
        "cold_admissions": len(value["entries"]) - len(prepared), "producer_inventory_unchanged": same_inventory,
        "restore_seconds": time.monotonic() - started, "new_paid_requests": 0}


def check(checkpoint_ref, *, progress=None):
    started = time.monotonic()
    manifest_ref, restored = restore(checkpoint_ref)
    # Recompute coverage, prior accounting and held reservations every time.
    validation = recovery.validate_campaign(r.read(manifest_ref), progress=progress)
    return {**restored, "state": "checkpoint_checked_offline", "validation": validation,
            "check_seconds": time.monotonic() - started}


def run(checkpoint_ref, *, allow_paid_api=False, canary_only=False):
    if not allow_paid_api:
        raise r.Error("checkpoint resume requires --allow-paid-api")
    manifest_ref, restored = restore(checkpoint_ref)
    print(r.canonical(restored).decode().strip(), flush=True)
    # Existing workspace lock, live receipts, reconciliation, phase gate and
    # global spending checks still belong exclusively to the original runner.
    return campaign.run(manifest_ref["path"], manifest_ref["sha256"],
                        allow_paid_api=allow_paid_api, canary_only=canary_only)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="command", required=True)
    build = modes.add_parser("prepare")
    build.add_argument("--manifest", required=True)
    build.add_argument("--expected-sha256", required=True)
    build.add_argument("--output", required=True)
    for name in ("check", "run"):
        sub = modes.add_parser(name)
        sub.add_argument("--checkpoint", required=True)
        sub.add_argument("--expected-sha256", required=True)
        if name == "run":
            sub.add_argument("--allow-paid-api", action="store_true")
            sub.add_argument("--canary-only", action="store_true")
    args = parser.parse_args(argv)
    def progress(index):
        print(r.canonical({"state": "validating_recovery_checkpoint", "producer_shard": index}).decode().strip(), flush=True)
    if args.command == "prepare":
        result = prepare({"path": args.manifest, "sha256": args.expected_sha256}, args.output, progress=progress)
    else:
        ref = {"path": args.checkpoint, "sha256": args.expected_sha256}
        result = check(ref, progress=progress) if args.command == "check" else run(ref,
            allow_paid_api=args.allow_paid_api, canary_only=args.canary_only)
    print(r.canonical(result).decode().strip(), flush=True)
    return 0 if args.command != "run" or result["state"] in {"completed", "awaiting_canary_review", "paused"} else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, OSError, KeyError, TypeError) as error:
        print("Recovery checkpoint stopped: " + str(error), file=sys.stderr)
        raise SystemExit(2)
