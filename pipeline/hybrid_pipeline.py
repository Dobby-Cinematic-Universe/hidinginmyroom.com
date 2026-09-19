"""Standby-first CPU/cloud sidecar. Existing campaign code and state stay untouched.

Only explicit, finished-media handoffs are discovered; an unchanged file is not
proof that a livestream has finished. No network or inference occurs at import,
initialization, scan, status, or readiness inspection.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import tempfile
import time

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import salad_transcription as salad
from pipeline.salad_transcription_contract import build_plan, canonical_bytes, load_recording_input
from pipeline.whispercpp_engine_profiles import ENGINE_PROFILES
from pipeline.preprocess_asr_queue import EXPECTED_MODEL, EXPECTED_MODEL_SHA256


ROUTES = {"archive_batch": "cloud", "new_video": "cpu", "finished_livestream": "cpu"}
MAX_HANDOFFS = 10000
MAX_INSPECTED = 32
MAX_HANDOFF_BYTES = 65536
MAX_SOURCE_BYTES = 64 * 1024**3
KINDS = {"pending", "preparing", "ready", "cpu_running", "cloud_running", "completed", "held", "failed"}
PIPELINE_ROOT = Path(__file__).resolve().parent
SOFTWARE = (
    "hybrid_pipeline.py", "hybrid_audio_prepare.py", "hybrid_cpu_worker.py", "hybrid_legacy_guard.py",
    "asr_whispercpp.py", "whispercpp_engine_profiles.py", "preprocess_asr_queue.py", "longform_asr_input.py",
    "salad_transcription.py", "salad_transcription_contract.py", "salad_transcription_client.py",
    "salad_transcription_temp_upload.py",
)


class HybridError(RuntimeError):
    pass


def digest(value: dict) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def seal(core: dict, id_key: str, prefix: str) -> dict:
    identity = digest(core)
    return {**core, "identity_sha256": identity, id_key: prefix + identity[:32]}


def _exact(value, keys, label):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise HybridError(f"invalid {label} fields")
    return value


def _integer(value, label, minimum=1, maximum=10000):
    if type(value) is not int or not minimum <= value <= maximum:
        raise HybridError(f"invalid {label} bound")
    return value


def _sha(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise HybridError("invalid SHA-256 binding")
    return value


def _money(value, label, *, fraction_digits=6, integer_digits=9):
    pattern = rf"[0-9]{{1,{integer_digits}}}(?:\.[0-9]{{1,{fraction_digits}}})?"
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise HybridError(f"invalid {label}")
    parsed = Decimal(value)
    if parsed <= 0:
        raise HybridError(f"{label} must be positive")
    return parsed


def _path(value):
    return salad._path(value)


def _no_overlap(left: Path, right: Path):
    return left != right and left not in right.parents and right not in left.parents


def software_bindings():
    return [{"path": str(PIPELINE_ROOT / name), "sha256": salad.file_sha256(PIPELINE_ROOT / name)} for name in SOFTWARE]


def verify_software(config):
    if software_bindings() != config["software"]:
        raise HybridError("hybrid software changed; review before further dispatch")


def validate_config(value: dict, *, verify_software: bool = True) -> dict:
    _exact(value, {"kind", "schema_version", "state_root", "inboxes", "legacy_guards", "tools", "cpu", "cloud",
                   "limits", "software", "policy", "identity_sha256", "config_id"}, "hybrid configuration")
    core = {key: item for key, item in value.items() if key not in {"identity_sha256", "config_id"}}
    if (value["kind"] != "himr_hybrid_pipeline_config" or value["schema_version"] != 1
            or seal(core, "config_id", "hybridcfg_") != value):
        raise HybridError("hybrid config identity differs")
    root = _path(value["state_root"])
    if len(root.parts) < 5 or root in {Path.cwd(), Path.home(), PIPELINE_ROOT.parent, Path("/mnt/archive/HIMR")}:
        raise HybridError("hybrid state must use a dedicated private directory")
    if value["policy"] != {"activation": "explicit_standby_first", "legacy_state_writes": False,
                            "source_deletion": False, "catalogue_writes": False, "publication": False,
                            "finished_handoffs_only": True, "same_source_single_route": True}:
        raise HybridError("hybrid safety policy differs")
    if not isinstance(value["legacy_guards"], list) or not value["legacy_guards"]:
        raise HybridError("legacy protection must not be empty")
    for guard in value["legacy_guards"]:
        _exact(guard, {"control_path", "status_path", "companion_status_path", "controller_lock", "companion_lock"}, "legacy guard")
        for path in guard.values():
            if not _no_overlap(root, _path(path).parent):
                raise HybridError("hybrid state overlaps legacy state")
    if not isinstance(value["inboxes"], list) or not 1 <= len(value["inboxes"]) <= 32:
        raise HybridError("configuration needs bounded completion inboxes")
    if len(set(value["inboxes"])) != len(value["inboxes"]):
        raise HybridError("duplicate completion inbox")
    for inbox in value["inboxes"]:
        path = _path(inbox)
        if path != root / "inbox" and (root in path.parents or path in root.parents or path == root):
            raise HybridError("external inbox overlaps runtime state")
    _exact(value["tools"], {"ffmpeg", "ffprobe"}, "audio tools")
    for tool in value["tools"].values():
        _exact(tool, {"path", "sha256"}, "tool binding")
        if not _no_overlap(root, _path(tool["path"]).parent):
            raise HybridError("state overlaps a tool directory")
        _sha(tool["sha256"])
    cpu = _exact(value["cpu"], {"engine", "model", "threads", "window_seconds", "timeout_seconds"}, "CPU policy")
    _integer(cpu["threads"], "CPU threads", 1, 8)
    _integer(cpu["window_seconds"], "CPU window", 1, 1800)
    _integer(cpu["timeout_seconds"], "CPU timeout", 60, 7200)
    from pipeline.asr_whispercpp import validate_engine, validate_model
    validate_engine(cpu["engine"])
    validate_model(cpu["model"])
    for source in (cpu["engine"]["executable"], cpu["model"]["path"]):
        if not _no_overlap(root, _path(source).parent):
            raise HybridError("state overlaps CPU runtime assets")
    cloud = _exact(value["cloud"], {"organization", "engine", "rate_usd_per_hour", "max_estimated_total_cost_usd",
                                   "diarization", "summary_words", "max_inflight_jobs"}, "cloud policy")
    if cloud["organization"] is not None and (not isinstance(cloud["organization"], str)
            or re.fullmatch(r"[a-z][a-z0-9-]{0,61}[a-z0-9]", cloud["organization"]) is None):
        raise HybridError("invalid cloud organization")
    if cloud["engine"] not in {"transcribe", "transcription-lite"} or cloud["diarization"] not in {"none", "sentence", "word", "both"}:
        raise HybridError("invalid cloud features")
    _integer(cloud["summary_words"], "summary words", 0, 2000)
    if cloud["engine"] == "transcription-lite" and cloud["summary_words"]:
        raise HybridError("summaries require the full cloud engine")
    for key in ("rate_usd_per_hour", "max_estimated_total_cost_usd"):
        if cloud[key] is not None:
            _money(cloud[key], key)
    _integer(cloud["max_inflight_jobs"], "cloud in-flight jobs", 1, 4)
    limits = _exact(value["limits"], {"max_admissions_per_cycle", "max_source_bytes", "max_attempts", "poll_seconds"}, "cycle limits")
    _integer(limits["max_admissions_per_cycle"], "admissions", 1, 32)
    _integer(limits["max_source_bytes"], "source bytes", 1, MAX_SOURCE_BYTES)
    _integer(limits["max_attempts"], "attempts", 1, 10)
    _integer(limits["poll_seconds"], "poll seconds", 5, 300)
    if (not isinstance(value["software"], list) or len(value["software"]) != len(SOFTWARE)
            or not all(isinstance(row, dict) for row in value["software"])
            or {row.get("path") for row in value["software"]} != {str(PIPELINE_ROOT / name) for name in SOFTWARE}):
        raise HybridError("hybrid software inventory differs")
    for row in value["software"]:
        _exact(row, {"path", "sha256"}, "software binding")
        _sha(row["sha256"])
        if verify_software and salad.file_sha256(Path(row["path"])) != row["sha256"]:
            raise HybridError("hybrid software changed; review and create a new configuration")
    return value


def default_config(root: Path, controller_config: Path, companion_config: Path, *, organization=None,
                   rate=None, budget=None, inboxes=None) -> dict:
    legacy = salad.read_json(controller_config)
    companion = salad.read_json(companion_config)
    def protect_configured_roots(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if (key.endswith("_root") and isinstance(item, str) and item.startswith("/")
                        and not _no_overlap(root, _path(item))):
                    raise HybridError("hybrid state overlaps a configured legacy data root")
                protect_configured_roots(item)
        elif isinstance(value, list):
            for item in value:
                protect_configured_roots(item)
    protect_configured_roots(legacy)
    protect_configured_roots(companion)
    legacy_root = _path(legacy["state_root"])
    deployment = companion["deployment"]
    profile = next(row for row in ENGINE_PROFILES if row["admission"] == "current_new_batch")
    repository = PIPELINE_ROOT.parent
    engine_path = repository / "research/corpus/tools/whisper.cpp-v1.8.7/build-noccache/bin/whisper-cli"
    model_path = repository / "research/corpus/tools/whisper.cpp/models/ggml-small.en.bin"
    core = {"kind": "himr_hybrid_pipeline_config", "schema_version": 1, "state_root": str(root),
            "inboxes": [str(path) for path in (inboxes or [root / "inbox"])],
            "legacy_guards": [{"control_path": str(legacy_root / "control.json"), "status_path": str(legacy_root / "status.json"),
                               "controller_lock": str(legacy_root / "controller.lock"),
                               "companion_status_path": deployment["status_path"], "companion_lock": deployment["dispatch_lock"]}],
            "tools": {name: {"path": f"/usr/bin/{name}", "sha256": salad.file_sha256(Path(f"/usr/bin/{name}"))} for name in ("ffmpeg", "ffprobe")},
            "cpu": {"engine": {"executable": str(engine_path), "expected_sha256": profile["expected_sha256"],
                               "version_label": profile["version_label"], "version_evidence": profile["version_evidence"], "build": profile["build"]},
                    "model": {"path": str(model_path), "expected_sha256": EXPECTED_MODEL_SHA256, **EXPECTED_MODEL},
                    "threads": 4, "window_seconds": 1800, "timeout_seconds": 7200},
            "cloud": {"organization": organization, "engine": "transcribe", "rate_usd_per_hour": rate,
                      "max_estimated_total_cost_usd": budget, "diarization": "both", "summary_words": 200, "max_inflight_jobs": 2},
            "limits": {"max_admissions_per_cycle": 8, "max_source_bytes": MAX_SOURCE_BYTES, "max_attempts": 3, "poll_seconds": 30},
            "software": software_bindings(),
            "policy": {"activation": "explicit_standby_first", "legacy_state_writes": False, "source_deletion": False,
                       "catalogue_writes": False, "publication": False, "finished_handoffs_only": True, "same_source_single_route": True}}
    return validate_config(seal(core, "config_id", "hybridcfg_"))


def make_handoff(source: dict, origin: str, source_key: str) -> dict:
    if origin not in ROUTES or not isinstance(source_key, str) or not 1 <= len(source_key) <= 512 or any(ord(c) < 32 for c in source_key):
        raise HybridError("invalid finished-source identity or origin")
    _exact(source, {"path", "sha256", "byte_count", "media_id"}, "source")
    _path(source["path"])
    _sha(source["sha256"])
    _integer(source["byte_count"], "source bytes", 1, MAX_SOURCE_BYTES)
    if source["media_id"] != "media_sha256_" + source["sha256"]:
        raise HybridError("source media identity must match its bound bytes")
    return seal({"kind": "himr_hybrid_finished_media", "schema_version": 1, "origin": origin,
                 "source_key": source_key, "finished": True, "source": source,
                 "policy": {"source_deletion": False, "publication": False}}, "handoff_id", "hybridhandoff_")


def validate_handoff(value):
    _exact(value, {"kind", "schema_version", "origin", "source_key", "finished", "source", "policy", "identity_sha256", "handoff_id"}, "finished-media handoff")
    if make_handoff(value["source"], value["origin"], value["source_key"]) != value:
        raise HybridError("finished-media handoff differs from deterministic replay")
    return value


def read_handoff(path):
    with salad._open_file(path) as descriptor:
        before = os.fstat(descriptor)
        if not 0 < before.st_size <= MAX_HANDOFF_BYTES:
            raise HybridError("handoff exceeds its bounded metadata size")
        body = os.read(descriptor, MAX_HANDOFF_BYTES + 1)
        if len(body) != before.st_size or salad._witness(before) != salad._witness(os.fstat(descriptor)):
            raise HybridError("handoff changed while being read")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise HybridError("duplicate handoff JSON field")
            result[key] = value
        return result
    return validate_handoff(json.loads(body, object_pairs_hook=pairs))


@contextmanager
def _lock(path: Path):
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o077:
            raise HybridError("invalid private hybrid lock")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise HybridError("another hybrid command owns this resource") from None
        yield
    finally:
        os.close(descriptor)


class Store:
    def __init__(self, config: dict):
        self.config = config
        self.root = _path(config["state_root"])
        self.connection = None
        self.lease_fds = ()

    def initialize(self):
        salad._private_directory(self.root)
        # Never add even a lock file to an unrelated existing workspace.
        existing = {path.name for path in self.root.iterdir()}
        if existing and ("config.json" not in existing or salad.read_json(self.root / "config.json") != self.config):
            raise HybridError("choose an empty dedicated hybrid workspace")
        if existing - {"config.json", "inbox", "jobs", "ledger.sqlite3", "ledger.sqlite3-journal", "initialize.lock",
                       "initialized.json", "control.json", "control.lock", "cycle.lock"}:
            raise HybridError("unrecognized hybrid workspace evidence; inspect before recovery")
        with _lock(self.root / "initialize.lock"):
            binding = self.root / "config.json"
            salad._write_json(binding, self.config)
            for name in ("inbox", "jobs"):
                salad._private_directory(self.root / name)
            database = self.root / "ledger.sqlite3"
            if not database.exists():
                if (self.root / "initialized.json").exists() or any((self.root / "jobs").iterdir()):
                    raise HybridError("hybrid ledger is missing; restore it instead of resetting paid work")
                descriptor, temporary = tempfile.mkstemp(prefix=".hybrid-ledger-", dir=self.root)
                os.close(descriptor)
                staging = Path(temporary)
                connection = sqlite3.connect(staging)
                try:
                    connection.executescript("""
                    PRAGMA journal_mode=DELETE;
                    PRAGMA synchronous=FULL;
                    CREATE TABLE binding(config_id TEXT NOT NULL);
                    CREATE TABLE jobs(
                        job_id TEXT PRIMARY KEY, media_sha256 TEXT NOT NULL UNIQUE, source_key TEXT NOT NULL UNIQUE,
                        origin TEXT NOT NULL, route TEXT NOT NULL, handoff_json TEXT NOT NULL,
                        status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                        prepared_json TEXT, cloud_plan_json TEXT, reserved_cost TEXT,
                        result_json TEXT, error_type TEXT
                    );
                    CREATE TABLE seen(handoff_id TEXT PRIMARY KEY, disposition TEXT NOT NULL, job_id TEXT);
                    CREATE TABLE cursors(name TEXT PRIMARY KEY, value TEXT NOT NULL);
                    """)
                    connection.execute("INSERT INTO binding VALUES (?)", (self.config["config_id"],))
                    connection.commit()
                finally:
                    connection.close()
                with salad._open_file(staging) as descriptor:
                    os.fsync(descriptor)
                # Publish only a fully committed database. A pre-publication
                # crash leaves evidence requiring inspection, not a blank ledger.
                os.link(staging, database, follow_symlinks=False)
                staging.unlink()
                salad._sync_directory(self.root)
            with self.database():
                pass
            if not (self.root / "control.json").exists():
                if (self.root / "initialized.json").exists():
                    raise HybridError("hybrid control state is missing")
                salad._write_json(self.root / "control.json", {"kind": "himr_hybrid_control", "schema_version": 1,
                                  "config_id": self.config["config_id"], "desired": "stopped", "generation": 0})
            salad._write_json(self.root / "initialized.json", {"config_id": self.config["config_id"], "initialized": True})

    @contextmanager
    def database(self, *, writable=False):
        salad._private_directory(self.root, create=False)
        if salad.read_json(self.root / "config.json") != self.config:
            raise HybridError("hybrid workspace belongs to another configuration")
        path = self.root / "ledger.sqlite3"
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o077:
            raise HybridError("invalid private hybrid ledger")
        connection = sqlite3.connect(path.as_uri() + f"?mode={'rw' if writable else 'ro'}", uri=True, timeout=1)
        connection.row_factory = sqlite3.Row
        self.connection = connection
        try:
            if [tuple(row) for row in connection.execute("SELECT config_id FROM binding")] != [(self.config["config_id"],)]:
                raise HybridError("ledger configuration binding differs")
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if tables != {"binding", "jobs", "seen", "cursors"}:
                raise HybridError("hybrid ledger schema differs")
            if writable:
                connection.execute("PRAGMA synchronous=FULL")
            self._audit_ledger()
            yield self
        finally:
            self.connection = None
            connection.close()

    def _audit_ledger(self):
        """Fail closed on partial ledger restores before any new paid work.

        Immutable claims precede mutable ledger commits. Evidence ahead of the
        ledger requires deliberate recovery; it can never become a fresh job.
        """
        jobs_root = salad._private_directory(self.root / "jobs", create=False)
        rows = self.connection.execute("SELECT * FROM jobs LIMIT ?", (MAX_HANDOFFS + 1,)).fetchall()
        if len(rows) > MAX_HANDOFFS:
            raise HybridError("job ledger exceeds bounded inventory")
        evidence = set()
        with os.scandir(jobs_root) as entries:
            for entry in entries:
                evidence.add(entry.name)
                if len(evidence) > MAX_HANDOFFS:
                    raise HybridError("job evidence exceeds bounded inventory")
        if evidence != {row["job_id"] for row in rows}:
            raise HybridError("ledger and immutable job claims differ; inspect recovery evidence")
        total = Decimal(0)
        for row in rows:
            handoff = validate_handoff(json.loads(row["handoff_json"]))
            source = handoff["source"]
            if (row["job_id"] != "hybridjob_" + source["sha256"][:32]
                    or row["media_sha256"] != source["sha256"] or row["source_key"] != handoff["source_key"]
                    or row["origin"] != handoff["origin"] or row["route"] != ROUTES[handoff["origin"]]
                    or row["status"] not in KINDS):
                raise HybridError("ledger source or route binding differs")
            _integer(row["attempts"], "recorded attempts", 0, 2**31-1)
            directory = salad._private_directory(jobs_root / row["job_id"], create=False)
            expected = {"config_id": self.config["config_id"], "job_id": row["job_id"], "handoff": handoff}
            if salad.read_json(directory / "admission.json") != expected:
                raise HybridError("immutable job admission differs")
            reservation = directory / "cloud-reservation.json"
            plan_path = directory / "cloud-plan.json"
            started = directory / "cloud-started.json"
            if row["cloud_plan_json"] is None:
                if row["reserved_cost"] is not None or any(path.exists() or path.is_symlink() for path in (reservation, plan_path, started, directory / "cloud")):
                    raise HybridError("unledgered cloud reservation or submission evidence")
                continue
            if row["route"] != "cloud":
                raise HybridError("CPU job contains a cloud reservation")
            binding = json.loads(row["cloud_plan_json"])
            _exact(binding, {"path", "sha256"}, "cloud plan binding")
            if binding["path"] != str(plan_path):
                raise HybridError("cloud plan escaped its job directory")
            plan = salad.load_plan(plan_path, _sha(binding["sha256"]))
            cost = _money(row["reserved_cost"], "recorded cloud reservation", fraction_digits=8, integer_digits=12)
            prepared = json.loads(row["prepared_json"])
            if (str(cost) != plan["estimate"]["estimated_cost_usd"]
                    or plan["output_root"] != str(directory / "cloud")
                    or len(plan["recordings"]) != 1
                    or plan["recordings"][0]["media_id"] != source["media_id"]
                    or plan["recordings"][0]["manifest"] != {"path": prepared["recording_input"], "sha256": prepared["sha256"]}):
                raise HybridError("cloud reservation or recording binding differs")
            expected_reservation = {"config_id": self.config["config_id"], "job_id": row["job_id"],
                                    "plan": binding, "reserved_cost": row["reserved_cost"]}
            if salad.read_json(reservation) != expected_reservation:
                raise HybridError("immutable cloud reservation differs")
            cloud_root = directory / "cloud"
            if started.exists() or started.is_symlink():
                if salad.read_json(started) != {"config_id": self.config["config_id"], "job_id": row["job_id"], "plan": binding}:
                    raise HybridError("cloud initialization witness differs")
                salad._private_directory(cloud_root, create=False)
                if salad.read_json(cloud_root / "plan.json") != plan:
                    raise HybridError("started cloud workspace plan differs")
                # Missing state cannot be interpreted as a never-submitted job.
                state = salad.read_json(cloud_root / "state.json")
                if state.get("plan_id") != plan["plan_id"]:
                    raise HybridError("started cloud runtime state differs")
            elif cloud_root.exists() or cloud_root.is_symlink():
                raise HybridError("cloud workspace exists without its initialization witness")
            elif row["status"] in {"cloud_running", "completed"} or row["result_json"] is not None:
                raise HybridError("previously started cloud workspace evidence is missing")
            total += cost
        budget = self.config["cloud"]["max_estimated_total_cost_usd"]
        if total and (budget is None or total > Decimal(budget)):
            raise HybridError("recorded cloud reservations exceed configured budget")

    def control(self):
        value = salad.read_json(self.root / "control.json")
        _exact(value, {"kind", "schema_version", "config_id", "desired", "generation"}, "hybrid control")
        if (value["kind"] != "himr_hybrid_control" or value["schema_version"] != 1
                or value["config_id"] != self.config["config_id"] or value["desired"] not in {"running", "stopped"}):
            raise HybridError("invalid hybrid control")
        _integer(value["generation"], "control generation", 0, 2**63-1)
        return value

    def set_control(self, desired):
        with _lock(self.root / "control.lock"):
            value = self.control()
            value.update(desired=desired, generation=value["generation"] + 1)
            salad._write_json(self.root / "control.json", value, replace=True)
        return value

    def summary(self):
        rows = self.connection.execute("SELECT status, route, COUNT(*) AS n FROM jobs GROUP BY status,route").fetchall()
        reserved = sum((Decimal(row[0]) for row in self.connection.execute("SELECT reserved_cost FROM jobs WHERE reserved_cost IS NOT NULL")), Decimal(0))
        return {"config_id": self.config["config_id"], "control": self.control(),
                "jobs": [{"route": row["route"], "status": row["status"], "count": row["n"]} for row in rows],
                "reserved_estimated_cloud_cost_usd": str(reserved), "actual_spend_cap": False,
                "inboxes": self.config["inboxes"], "legacy_state_modified": False}

    def scan(self):
        self._audit_ledger()
        admitted = duplicates = rejected = 0
        checked = 0
        maximum = self.config["limits"]["max_admissions_per_cycle"]
        candidates = []
        inventory_entries = 0
        for inbox in self.config["inboxes"]:
            path = _path(inbox)
            salad._private_directory(path, create=False)
            with os.scandir(path) as entries:
                for entry in entries:
                    inventory_entries += 1
                    if inventory_entries > MAX_HANDOFFS:
                        raise HybridError("completion inbox exceeds its bounded inventory")
                    if entry.name.endswith(".json"):
                        candidates.append(Path(entry.path))
        cursor = self.connection.execute("SELECT value FROM cursors WHERE name='scan'").fetchone()
        last = cursor[0] if cursor else ""
        candidates.sort(key=lambda path: (str(path) <= last, str(path)))
        for candidate in candidates:
            if admitted >= maximum or checked >= MAX_INSPECTED:
                break
            checked += 1
            with self.connection:
                self.connection.execute("INSERT OR REPLACE INTO cursors VALUES('scan',?)", (str(candidate),))
            try:
                value = read_handoff(candidate)
                if self.connection.execute("SELECT 1 FROM seen WHERE handoff_id=?", (value["handoff_id"],)).fetchone():
                    continue
                source = value["source"]
                if source["byte_count"] > self.config["limits"]["max_source_bytes"]:
                    raise HybridError("source exceeds the configured byte limit")
                if not _no_overlap(self.root, Path(source["path"]).parent):
                    raise HybridError("handoff source overlaps runtime artifacts")
                # Metadata only. Source contents are verified at normalization.
                with salad._open_file(Path(source["path"])) as descriptor:
                    if os.fstat(descriptor).st_size != source["byte_count"]:
                        raise HybridError("finished-source size differs")
                job_id = "hybridjob_" + source["sha256"][:32]
                previous = self.connection.execute("SELECT job_id,media_sha256,source_key,route FROM jobs WHERE media_sha256=? OR source_key=?",
                                                   (source["sha256"], value["source_key"])).fetchall()
                with self.connection:
                    if previous:
                        exact = len(previous) == 1 and previous[0]["media_sha256"] == source["sha256"] and previous[0]["route"] == ROUTES[value["origin"]]
                        disposition = "duplicate" if exact else "conflicting_source_or_route"
                        self.connection.execute("INSERT INTO seen VALUES(?,?,?)", (value["handoff_id"], disposition, previous[0]["job_id"]))
                        duplicates += 1
                    else:
                        if self.connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] >= MAX_HANDOFFS:
                            raise HybridError("job ledger reached its bounded inventory")
                        directory = salad._private_directory(self.root / "jobs" / job_id)
                        salad._write_json(directory / "admission.json", {"config_id": self.config["config_id"],
                                          "job_id": job_id, "handoff": value})
                        salad._sync_directory(self.root / "jobs")
                        self.connection.execute("INSERT INTO jobs(job_id,media_sha256,source_key,origin,route,handoff_json,status) VALUES(?,?,?,?,?,?,?)",
                                                (job_id, source["sha256"], value["source_key"], value["origin"], ROUTES[value["origin"]], canonical_bytes(value).decode(), "pending"))
                        self.connection.execute("INSERT INTO seen VALUES(?,?,?)", (value["handoff_id"], "admitted", job_id))
                        admitted += 1
            except (HybridError, salad.CloudPipelineError, OSError, ValueError, TypeError):
                rejected += 1
                # A failure after claim publication may have left immutable
                # evidence ahead of SQLite. Never process around that hole.
                self._audit_ledger()
        return {"admitted": admitted, "duplicates_or_conflicts": duplicates, "rejected": rejected, "metadata_inspected": checked, "media_hashed": False}

    def update_job(self, job_id: str, **fields):
        allowed = {"status", "attempts", "prepared_json", "cloud_plan_json", "reserved_cost", "result_json", "error_type"}
        if not fields or not set(fields) <= allowed or ("status" in fields and fields["status"] not in KINDS):
            raise HybridError("invalid job update")
        with self.connection:
            self.connection.execute("UPDATE jobs SET " + ",".join(key + "=?" for key in fields) + " WHERE job_id=?", (*fields.values(), job_id))

    def _prepare(self, job: dict, check):
        from pipeline.hybrid_audio_prepare import prepare_audio
        handoff = validate_handoff(json.loads(job["handoff_json"]))
        def validate_prepared(prepared):
            manifest = _path(prepared["recording_input"])
            row = load_recording_input(manifest, _sha(prepared["sha256"]))
            audio_root = self.root / "jobs" / job["job_id"] / "audio"
            if (audio_root not in manifest.parents or audio_root not in _path(row["audio"]["path"]).parents
                    or row["media_id"] != handoff["source"]["media_id"]):
                raise HybridError("prepared audio belongs to another source or job")
            return prepared
        if job["prepared_json"] is not None:
            prepared = json.loads(job["prepared_json"])
            return validate_prepared(prepared)
        check()
        self.update_job(job["job_id"], status="preparing")
        prepared = prepare_audio(handoff["source"], output_root=self.root / "jobs" / job["job_id"] / "audio",
                                 ffmpeg=self.config["tools"]["ffmpeg"], ffprobe=self.config["tools"]["ffprobe"], timeout_seconds=7200,
                                 lease_fds=self.lease_fds)
        validate_prepared(prepared)
        self.update_job(job["job_id"], status="ready", prepared_json=canonical_bytes(prepared).decode())
        return prepared

    def _cpu(self, job, check):
        from pipeline.hybrid_cpu_worker import run_cpu
        prepared = self._prepare(job, check)
        check()
        self.update_job(job["job_id"], status="cpu_running")
        result = run_cpu(Path(prepared["recording_input"]), prepared["sha256"],
                         output_root=self.root / "jobs" / job["job_id"] / "cpu",
                         engine=self.config["cpu"]["engine"], model=self.config["cpu"]["model"],
                         ffmpeg=self.config["tools"]["ffmpeg"], ffprobe=self.config["tools"]["ffprobe"],
                         threads=self.config["cpu"]["threads"], timeout_seconds=self.config["cpu"]["timeout_seconds"],
                         window_seconds=self.config["cpu"]["window_seconds"], max_windows=1, lease_fds=self.lease_fds)
        self.update_job(job["job_id"], status="completed" if result["status"] == "completed" else "ready", result_json=canonical_bytes(result).decode())

    def _cloud_plan(self, job, prepared):
        self._audit_ledger()
        if job["cloud_plan_json"] is not None:
            binding = json.loads(job["cloud_plan_json"])
            return salad.load_plan(Path(binding["path"]), binding["sha256"])
        policy = self.config["cloud"]
        if any(policy[key] is None for key in ("organization", "rate_usd_per_hour", "max_estimated_total_cost_usd")):
            raise HybridError("cloud organization, reviewed rate, and estimated budget are not configured")
        row = load_recording_input(Path(prepared["recording_input"]), prepared["sha256"])
        plan = build_plan([row], organization=policy["organization"], engine=policy["engine"],
                          output_root=str(self.root / "jobs" / job["job_id"] / "cloud"), ffmpeg=self.config["tools"]["ffmpeg"],
                          rate_usd_per_hour=policy["rate_usd_per_hour"], max_estimated_cost_usd=policy["max_estimated_total_cost_usd"],
                          diarization=policy["diarization"] in {"word", "both"}, sentence_diarization=policy["diarization"] in {"sentence", "both"},
                          summary_words=policy["summary_words"])
        cost = Decimal(plan["estimate"]["estimated_cost_usd"])
        reserved = sum((Decimal(row[0]) for row in self.connection.execute("SELECT reserved_cost FROM jobs WHERE reserved_cost IS NOT NULL")), Decimal(0))
        if reserved + cost > Decimal(policy["max_estimated_total_cost_usd"]):
            raise HybridError("cumulative estimated cloud budget would be exceeded")
        path = self.root / "jobs" / job["job_id"] / "cloud-plan.json"
        salad._write_json(path, plan)
        binding = {"path": str(path), "sha256": digest(plan)}
        salad._write_json(path.parent / "cloud-reservation.json", {"config_id": self.config["config_id"],
                          "job_id": job["job_id"], "plan": binding, "reserved_cost": str(cost)})
        # Reserve the whole recording before the first possible paid POST. Never
        # release a reservation automatically, including failed/unknown jobs.
        self.update_job(job["job_id"], cloud_plan_json=canonical_bytes(binding).decode(), reserved_cost=str(cost))
        return plan

    def _cloud_active(self):
        active = 0
        ambiguous = False
        for row in self.connection.execute("SELECT cloud_plan_json FROM jobs WHERE cloud_plan_json IS NOT NULL"):
            binding = json.loads(row[0])
            plan = salad.load_plan(Path(binding["path"]), binding["sha256"])
            root = Path(plan["output_root"])
            if root.exists():
                with salad.Workspace(plan).locked(create=False) as workspace:
                    states = workspace.summary()["states"]
                    if states.get("submission_unknown", 0) or states.get("submitting", 0):
                        ambiguous = True
                    active += sum(states.get(status, 0) for status in salad.ACTIVE)
        return active, ambiguous

    def _next_job(self, route):
        cursor = self.connection.execute("SELECT value FROM cursors WHERE name=?", (route,)).fetchone()
        last = int(cursor[0]) if cursor else 0
        rows = self.connection.execute("SELECT rowid AS sequence,* FROM jobs WHERE route=? AND "
            "(status IN ('pending','preparing','ready','cpu_running','cloud_running') OR "
            "(route='cloud' AND status='held' AND cloud_plan_json IS NOT NULL)) "
            "ORDER BY CASE WHEN rowid>? THEN 0 ELSE 1 END,rowid LIMIT ?", (route, last, MAX_HANDOFFS)).fetchall()
        if route == "cloud":
            # A large fresh inbox must not delay retrieval of known provider
            # results. Round-robin among pollable recordings first, including
            # known chunks in otherwise held recordings.
            for row in rows:
                if row["cloud_plan_json"] is None:
                    continue
                binding = json.loads(row["cloud_plan_json"])
                plan = salad.load_plan(Path(binding["path"]), binding["sha256"])
                if not Path(plan["output_root"]).exists():
                    continue
                with salad.Workspace(plan).locked(create=False) as workspace:
                    states = workspace.summary()["states"]
                    if any(states.get(state, 0) for state in ("pending", "running", "succeeded")):
                        return row
            rows = [row for row in rows if row["status"] != "held"]
        return rows[0] if rows else None

    def _cloud(self, job, check):
        client = salad.SaladClient(self.config["cloud"]["organization"])
        active, ambiguous = self._cloud_active()
        # Continue polling known provider IDs even when some submission needs
        # reconciliation, without preparing/uploading any fresh recording.
        if job["cloud_plan_json"] is None and (ambiguous or active >= self.config["cloud"]["max_inflight_jobs"]):
            return
        prepared = self._prepare(job, check)
        check()
        plan = self._cloud_plan(job, prepared)
        check()
        binding = {"path": str(self.root / "jobs" / job["job_id"] / "cloud-plan.json"), "sha256": digest(plan)}
        salad._write_json(self.root / "jobs" / job["job_id"] / "cloud-started.json",
                          {"config_id": self.config["config_id"], "job_id": job["job_id"], "plan": binding})
        self.update_job(job["job_id"], status="cloud_running")
        with salad.Workspace(plan, lease_fds=self.lease_fds).locked() as workspace:
            states = workspace.summary()["states"]
            requires_review = any(states.get(state, 0) for state in ("submitting", "submission_unknown", "failed", "cancelled"))
            result = workspace.run_cycle(client, max_new_jobs=1 if not ambiguous and not requires_review and job["status"] != "held" and active < self.config["cloud"]["max_inflight_jobs"] else 0,
                                         max_inflight=self.config["cloud"]["max_inflight_jobs"], max_polls=4)
        states = result["states"]
        if states.get("submission_unknown", 0) or states.get("submitting", 0) or states.get("failed", 0) or states.get("cancelled", 0):
            status = "held"
        elif states.get("completed", 0) == result["chunks"]:
            status = "completed"
        else:
            status = "cloud_running"
        self.update_job(job["job_id"], status=status, result_json=canonical_bytes(result).decode())

    def cycle(self, *, allow_local=False, allow_cloud=False):
        from pipeline.hybrid_legacy_guard import hold_legacy, LegacyBusy
        if not allow_local:
            raise HybridError("execution requires --allow-local-processing")
        initial = self.control()
        if initial["desired"] != "running":
            return {"status": "standby", "legacy_state_modified": False}
        with _lock(self.root / "cycle.lock"):
            try:
                with hold_legacy(self.config["legacy_guards"]) as legacy:
                    self.lease_fds = legacy.inherited_fds
                    def check():
                        legacy.check_unchanged()
                        if self.control() != initial:
                            raise HybridError("hybrid control changed; no further work dispatched")
                        verify_software(self.config)
                    check()
                    scan = self.scan()
                    dispatched = []
                    for route in ("cpu", "cloud"):
                        if route == "cloud" and not allow_cloud:
                            continue
                        if route == "cloud" and any(self.config["cloud"][key] is None for key in ("organization", "rate_usd_per_hour", "max_estimated_total_cost_usd")):
                            continue
                        # Continue existing remote jobs before submitting another
                        # recording. CPU similarly resumes its bounded windows.
                        found = self._next_job(route)
                        if found is None:
                            continue
                        job = dict(found)
                        with self.connection:
                            self.connection.execute("INSERT OR REPLACE INTO cursors VALUES(?,?)", (route, str(job["sequence"])))
                        check()
                        try:
                            self._cpu(job, check) if route == "cpu" else self._cloud(job, check)
                            dispatched.append({"job_id": job["job_id"], "route": route})
                        except (LegacyBusy, KeyboardInterrupt):
                            raise
                        except Exception as error:
                            attempts = job["attempts"] + 1
                            self.update_job(job["job_id"], attempts=attempts, error_type=type(error).__name__,
                                            status="held" if job["status"] == "held" or attempts >= self.config["limits"]["max_attempts"] else "ready")
                            dispatched.append({"job_id": job["job_id"], "route": route, "error_type": type(error).__name__})
                    return {"status": "cycle_complete", "scan": scan, "dispatched": dispatched, **self.summary()}
            except LegacyBusy:
                return {"status": "held_legacy_active_or_uncertain", "legacy_state_modified": False}
            finally:
                self.lease_fds = ()


def readiness(config: dict):
    from pipeline.hybrid_legacy_guard import inspect_legacy
    missing = ["cloud." + key for key in ("organization", "rate_usd_per_hour", "max_estimated_total_cost_usd") if config["cloud"][key] is None]
    if not os.environ.get("SALAD_API_KEY"):
        missing.append("SALAD_API_KEY")
    assets = {}
    for name, path in {"cpu_engine": config["cpu"]["engine"]["executable"], "cpu_model": config["cpu"]["model"]["path"],
                       **{key: value["path"] for key, value in config["tools"].items()}}.items():
        try:
            with salad._open_file(Path(path)) as descriptor:
                assets[name] = {"present": True, "byte_count": os.fstat(descriptor).st_size, "content_hash_checked": False}
        except (OSError, salad.CloudPipelineError):
            assets[name] = {"present": False, "content_hash_checked": False}
    return {"config_id": config["config_id"], "legacy": inspect_legacy(config["legacy_guards"]),
            "assets": assets, "missing_cloud_configuration": missing,
            "discovery": "finished_media_handoff_inboxes_only", "live_stream_capture": False,
            "live_cloud_pilot_completed": False, "no_media_read": True, "no_network": True}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    initialize = commands.add_parser("init", help="create a separate private standby workspace; never activate")
    initialize.add_argument("--root", required=True, type=Path)
    initialize.add_argument("--legacy-controller-config", required=True, type=Path)
    initialize.add_argument("--legacy-companion-config", required=True, type=Path)
    initialize.add_argument("--inbox", action="append", type=Path)
    initialize.add_argument("--cloud-organization")
    initialize.add_argument("--cloud-rate-usd-per-hour")
    initialize.add_argument("--cloud-estimated-budget-usd")
    handoff = commands.add_parser("handoff", help="publish an explicit finished-media descriptor, without media reads")
    handoff.add_argument("--source", type=Path, required=True)
    handoff.add_argument("--source-sha256", required=True)
    handoff.add_argument("--source-bytes", type=int, required=True)
    handoff.add_argument("--source-key", required=True)
    handoff.add_argument("--origin", choices=tuple(ROUTES), required=True)
    handoff.add_argument("--output", type=Path, required=True)
    handoff.add_argument("--confirm-finished", action="store_true")
    for name in ("status", "readiness", "scan", "start", "stop", "cycle", "serve", "retry"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True, type=Path)
        command.add_argument("--config-sha256", required=True)
        if name in {"cycle", "serve"}:
            command.add_argument("--allow-local-processing", action="store_true")
            command.add_argument("--allow-cloud", action="store_true")
        if name == "serve":
            command.add_argument("--max-cycles", type=int, default=0, help="0 means supervise until stopped")
        if name == "retry":
            command.add_argument("--job-id", required=True)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "init":
            config = default_config(_path(args.root), _path(args.legacy_controller_config), _path(args.legacy_companion_config),
                                    organization=args.cloud_organization, rate=args.cloud_rate_usd_per_hour,
                                    budget=args.cloud_estimated_budget_usd, inboxes=args.inbox)
            Store(config).initialize()
            report = {"status": "standby", "config": str(Path(config["state_root"]) / "config.json"), "config_sha256": digest(config)}
        elif args.command == "handoff":
            if not args.confirm_finished:
                raise HybridError("handoff requires explicit --confirm-finished")
            value = make_handoff({"path": str(_path(args.source)), "sha256": args.source_sha256,
                                  "byte_count": args.source_bytes, "media_id": "media_sha256_" + args.source_sha256}, args.origin, args.source_key)
            output = _path(args.output)
            salad._private_directory(output.parent, create=False)
            salad._write_json(output, value)
            report = {"handoff_id": value["handoff_id"], "route": ROUTES[args.origin], "media_read": False}
        else:
            config = validate_config(salad.read_json(_path(args.config), args.config_sha256))
            store = Store(config)
            if args.command == "readiness":
                report = readiness(config)
            elif args.command in {"start", "stop"}:
                if args.command == "start":
                    from pipeline.hybrid_legacy_guard import inspect_legacy
                    if not inspect_legacy(config["legacy_guards"])["safe"]:
                        raise HybridError("legacy campaign is active or uncertain; hybrid remains in standby")
                report = store.set_control("running" if args.command == "start" else "stopped")
            else:
                with store.database(writable=args.command != "status"):
                    if args.command == "status":
                        report = store.summary()
                    elif args.command == "scan":
                        with _lock(store.root / "cycle.lock"):
                            report = store.scan()
                    elif args.command == "retry":
                        with _lock(store.root / "cycle.lock"):
                            row = store.connection.execute("SELECT status FROM jobs WHERE job_id=?", (args.job_id,)).fetchone()
                            if row is None or row[0] not in {"held", "failed"}:
                                raise HybridError("only a held/failed known job can be retried")
                            store.update_job(args.job_id, status="ready", attempts=0, error_type=None)
                            report = {"job_id": args.job_id, "status": "ready", "cloud_submission_intents_preserved": True}
                    elif args.command == "cycle":
                        report = store.cycle(allow_local=args.allow_local_processing, allow_cloud=args.allow_cloud)
                    else:
                        _integer(args.max_cycles, "maximum cycles", 0, 1000000)
                        cycles = 0
                        while True:
                            report = store.cycle(allow_local=args.allow_local_processing, allow_cloud=args.allow_cloud)
                            cycles += 1
                            print(json.dumps(report, sort_keys=True), flush=True)
                            if store.control()["desired"] == "stopped" or (args.max_cycles and cycles >= args.max_cycles):
                                return 0
                            deadline = time.monotonic() + config["limits"]["poll_seconds"]
                            while time.monotonic() < deadline:
                                if store.control()["desired"] == "stopped":
                                    return 0
                                time.sleep(min(0.5, max(0, deadline - time.monotonic())))
        print(json.dumps(report, sort_keys=True))
        return 0
    except (HybridError, salad.CloudPipelineError, salad.CloudContractError, salad.CloudClientError) as error:
        print(f"HybridPipelineError: {error}", file=sys.stderr)
        return 2
    except (OSError, ValueError, TypeError, KeyError, IndexError, sqlite3.Error, InvalidOperation):
        print("HybridPipelineError: private state, binding, or local I/O validation failed", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
