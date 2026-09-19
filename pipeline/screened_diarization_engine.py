"""Offline, single-recording Community-1 worker; never launched on import.

No downloads, credential handling, arbitrary YAML classes, unsafe-pickle fallback,
external audio paths, or chunk stitching are supported. The parent owns process,
wall-time and hard host-memory limits. A full waveform can still exceed model
working memory: admission is not a claim that long recordings fit a particular GPU.

Bundle manifest: kind=himr_community1_bundle, schema_version=1, repository,
revision, root, files[{relative_path,sha256,byte_count}], runtime binding, license
binding, review_evidence binding. Bindings have path/sha256; runtime file bindings
also have byte_count. All upstream files must be mirrored, including documentation.
Runtime manifest: kind=himr_community1_runtime, schema_version=1,
python{path,sha256,byte_count,version}, packages[{name,version,wheel}],
installed_files[{path,sha256,byte_count}]. It must cover all installed distributions
and their RECORD-listed files. Wheels and installed files are hashed before imports.
The bundle review binds the exact files digest, runtime and license bindings.

Reviewed sources (no source code copied): pyannote.audio 4.0.7 commit
b749285c5cdd4636b2edc7f766f1352c8dde9369, core/model.py, core/plda.py,
pipelines/speaker_diarization.py, pipelines/speaker_verification.py; model tree at
https://huggingface.co/pyannote/speaker-diarization-community-1/tree/
3533c8cf8e369892e6b79ff1bf80f7b0286a54ee . Config content is gated and has not
been natively validated here. Its Git blob identity is pinned independently below.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import copy
import hashlib
import importlib.metadata
import io
import json
import math
import numbers
import os
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import speaker_screen as safe

DiarizationError = safe.ScreenError
REPOSITORY = "pyannote/speaker-diarization-community-1"
REVISION = "3533c8cf8e369892e6b79ff1bf80f7b0286a54ee"
PYANNOTE_VERSION = "4.0.7"
PYANNOTE_WHEEL_SHA256 = "852ea15c4d85bc34773e618267603ffca6a521669a74d33742692cc67fc700d6"
MAX_JSON = 32 * 1024**2
MAX_DURATION_MS = 86_400_000
MAX_TURNS = 100_000
# size, Git blob identity, LFS storage. LFS identities bind the canonical pointer
# generated from the separately registered physical SHA-256 and physical size.
UPSTREAM_FILES = {
    ".gitattributes": (1571, "7c39301dc01fe65e09b2432a27a18b4ee3e74a37", False),
    "README.md": (9978, "8356d6634d7b1074581dd36e2225887ec809326e", False),
    "config.yaml": (444, "4022db43960736338378fdb6b5a85cfdae198910", False),
    "diarization.gif": (861445, "114f825e4f8e854ed9ebbf06fd75feaa68d16b88", True),
    "embedding/README.md": (938, "d71f943e00016652680bef1e6c49fbc842e0d4c4", False),
    "embedding/pytorch_model.bin": (26646242, "6347697826a1958253dd195fa296d8d97b8f6280", True),
    "plda/README.md": (220, "5c01ad2e673203fe32a82d7a140eb817db9930b7", False),
    "plda/plda.npz": (133852, "e61936ef2108eccdff801c440d5e3f6c6995aba4", True),
    "plda/xvec_transform.npz": (134376, "b8079f30a35a68fe7d996bd7abc2170338ba6767", True),
    "segmentation/pytorch_model.bin": (5906507, "e3b91de5a5374fc40b556f9cc51317d280b4ea79", True),
}
WEIGHT_SHA256 = {
    "segmentation/pytorch_model.bin": "7ad24338d844fb95985486eb1a464e32d229f6d7a03c9abe60f978bacf3f816e",
    "embedding/pytorch_model.bin": "6f10ff60898a1d185fa22e1d11e0bfa8a92efec811f11bca48cb8cafebefd929",
    "plda/plda.npz": "9b77bcd840692710dd3496f62ecfeed8d8e5f002fd991b785079b244eab7d255",
    "plda/xvec_transform.npz": "325f1ce8e48f7e55e9c8aa47e05d2766b7c48c4b25b8de8dd751e7a4cc5fbe8f",
}
ARCHITECTURES = {
    "segmentation": ("pyannote.audio.models.segmentation.PyanNet", "PyanNet"),
    "embedding": ("pyannote.audio.models.embedding.wespeaker", "WeSpeakerResNet34"),
}
ENVIRONMENT = {
    "HF_HUB_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1", "HF_HUB_DISABLE_TELEMETRY": "1",
    "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1", "PYANNOTE_METRICS_ENABLED": "false",
    "OTEL_SDK_DISABLED": "true", "DO_NOT_TRACK": "1", "TORCH_FORCE_WEIGHTS_ONLY_LOAD": "1",
    "NVIDIA_TF32_OVERRIDE": "0", "CUBLAS_WORKSPACE_CONFIG": ":4096:8",
}
SEMANTICS = {"full_recording_waveform": True, "external_chunk_stitching": False,
    "network_denied": True, "telemetry_disabled": True, "score_state": "unavailable",
    "speaker_embeddings_exported": False, "person_identity_claimed": False,
    "cross_recording_label_linking": False, "quality_gate_passed": False,
    "publication_authority": False, "float32": True, "tf32": False}


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                          allow_nan=False).encode()
    except (TypeError, ValueError, RecursionError) as error:
        raise DiarizationError("non-finite or invalid JSON") from error


def _binding(value, *, sized=False, allow_empty=False):
    safe.exact(value, {"path", "sha256", "byte_count"} if sized else {"path", "sha256"}, "binding")
    safe.file_binding({key: value[key] for key in ("path", "sha256")})
    if sized:
        safe.integer(value["byte_count"], 0 if allow_empty else 1, 32 * 1024**3, "bound byte count")


def _read(binding, maximum=MAX_JSON):
    _binding(binding)
    with safe.opened(binding["path"]) as fd:
        before = safe.witness(fd)
        if not 0 < before["st_size"] <= maximum:
            raise DiarizationError("bound artifact exceeds size limit")
        body = os.pread(fd, maximum + 1, 0)
        if len(body) != before["st_size"] or safe.witness(fd) != before:
            raise DiarizationError("bound artifact changed while reading")
    if hashlib.sha256(body).hexdigest() != binding["sha256"]:
        raise DiarizationError("artifact SHA-256 mismatch")
    return body, before


def _json(binding):
    body, witness = _read(binding)
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise DiarizationError("duplicate JSON field")
            value[key] = item
        return value
    try:
        value = json.loads(body, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(DiarizationError("nonfinite JSON")))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise DiarizationError("invalid JSON artifact") from error
    if not isinstance(value, dict):
        raise DiarizationError("JSON artifact must be an object")
    _canonical(value)
    return value, witness


def _version(value, kind, fields):
    safe.exact(value, {"kind", "schema_version", *fields}, kind)
    if value["kind"] != kind or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise DiarizationError("unsupported artifact kind/version")


def _git_blob(body):
    return hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest()


def _config(body):
    """Parse only a small mapping/scalar YAML subset; never import a YAML loader.

    The upstream Git identity is separately verified. No anchors, collections,
    tags, merge keys, quoted keys, multiline values or executable class extensions.
    """
    try:
        text = body.decode("utf-8")
    except UnicodeError as error:
        raise DiarizationError("invalid model config encoding") from error
    root, stack = {}, [(-2, {})]
    stack[0] = (-2, root)
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if "\t" in line or any(char in line for char in ("!", "&", "*", "[", "]", "{", "}", "\\")):
            raise DiarizationError("unreviewed YAML syntax")
        match = re.fullmatch(r"( *)([A-Za-z_][A-Za-z0-9_.-]*):(?: +(.*))?", line)
        if match is None:
            raise DiarizationError("unreviewed YAML mapping")
        spaces, key, raw = match.groups()
        depth = len(spaces)
        while depth <= stack[-1][0]:
            stack.pop()
        if depth != stack[-1][0] + 2 or depth > 8:
            raise DiarizationError("invalid model config indentation")
        parent = stack[-1][1]
        if key in parent:
            raise DiarizationError("duplicate model config field")
        if raw is None:
            value = {}
            stack.append((depth, value))
        elif raw in {"true", "false"}:
            value = raw == "true"
        elif re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", raw):
            value = float(raw) if "." in raw else int(raw)
        elif raw[:1] in {"'", '"'}:
            if len(raw) < 2 or raw[-1] != raw[0] or raw[0] in raw[1:-1]:
                raise DiarizationError("invalid config string")
            value = raw[1:-1]
        else:
            value = raw
        parent[key] = value
    safe.exact(root, {"version", "pipeline", "params"} if "version" in root else
               {"dependencies", "pipeline", "params"}, "model config")
    if "version" in root:
        if not isinstance(root["version"], str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", root["version"]):
            raise DiarizationError("unreviewed config version")
    elif (not isinstance(root["dependencies"], dict) or not root["dependencies"]
          or set(root["dependencies"]) - {"pyannote.audio", "pyannote.core", "pyannote.pipeline"}
          or any(not isinstance(v, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", v)
                 for v in root["dependencies"].values())):
        raise DiarizationError("unreviewed config dependencies")
    safe.exact(root["pipeline"], {"name", "params"}, "pipeline config")
    if root["pipeline"]["name"] != "pyannote.audio.pipelines.SpeakerDiarization":
        raise DiarizationError("unreviewed pipeline class")
    params = root["pipeline"]["params"]
    safe.exact(params, {"segmentation", "embedding", "plda", "clustering", "embedding_exclude_overlap"}, "pipeline parameters")
    for key in ("segmentation", "embedding", "plda"):
        if params[key] != "$model/" + key:
            raise DiarizationError("model config must use exact local subfolder references")
    if params["clustering"] != "VBxClustering" or type(params["embedding_exclude_overlap"]) is not bool:
        raise DiarizationError("unreviewed clustering or overlap recipe")
    safe.exact(root["params"], {"segmentation", "clustering"}, "inference parameters")
    safe.exact(root["params"]["segmentation"], {"min_duration_off"}, "segmentation parameters")
    safe.exact(root["params"]["clustering"], {"threshold", "Fa", "Fb"}, "clustering parameters")
    for value in [*root["params"]["segmentation"].values(), *root["params"]["clustering"].values()]:
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 2:
            raise DiarizationError("unreviewed numeric pipeline parameter")
    return root


def _runtime_schema(value):
    _version(value, "himr_community1_runtime", {"python", "packages", "installed_files"})
    safe.exact(value["python"], {"path", "sha256", "byte_count", "version"}, "runtime Python")
    _binding({k: value["python"][k] for k in ("path", "sha256", "byte_count")}, sized=True)
    if not isinstance(value["python"]["version"], str) or not re.fullmatch(r"3\.[0-9]+\.[0-9]+", value["python"]["version"]):
        raise DiarizationError("exact Python version required")
    packages, files = value["packages"], value["installed_files"]
    if not isinstance(packages, list) or not 1 <= len(packages) <= 512:
        raise DiarizationError("bounded complete package inventory required")
    if not isinstance(files, list) or not 1 <= len(files) <= 100_000:
        raise DiarizationError("bounded installed file inventory required")
    names, paths = set(), set()
    for row in packages:
        safe.exact(row, {"name", "version", "wheel"}, "runtime package")
        name = row["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", name) or name in names:
            raise DiarizationError("duplicate or noncanonical package name")
        names.add(name)
        if not isinstance(row["version"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.+_-]{0,79}", row["version"]):
            raise DiarizationError("exact package version required")
        _binding(row["wheel"], sized=True)
        if name == "pyannote-audio" and (row["version"] != PYANNOTE_VERSION or
                row["wheel"]["sha256"] != PYANNOTE_WHEEL_SHA256 or row["wheel"]["byte_count"] != 894598):
            raise DiarizationError("unreviewed pyannote runtime wheel")
    if not {"pyannote-audio", "torch", "torchaudio", "numpy", "lightning", "pyannote-core", "pyannote-pipeline"} <= names:
        raise DiarizationError("required runtime packages missing")
    for row in files:
        _binding(row, sized=True, allow_empty=True)
        if row["path"] in paths:
            raise DiarizationError("duplicate installed file")
        paths.add(row["path"])
    if sum(row["byte_count"] for row in files) > 32 * 1024**3:
        raise DiarizationError("runtime inventory exceeds byte budget")
    return value


def _runtime_files_present(runtime):
    """Readiness checks existence/size only; the child hashes every file later."""
    rows = [*runtime["installed_files"], *[row["wheel"] for row in runtime["packages"]],
            {key: runtime["python"][key] for key in ("path", "sha256", "byte_count")}]
    for row in rows:
        with safe.opened(row["path"]) as fd:
            if safe.witness(fd)["st_size"] != row["byte_count"]:
                raise DiarizationError("registered runtime file size differs")


def admit_bundle(binding):
    """Read-only artifact admission, not a native/quality/readiness claim."""
    value, manifest_witness = _json(binding)
    _version(value, "himr_community1_bundle", {"repository", "revision", "root", "files", "runtime", "license", "review_evidence"})
    if value["repository"] != REPOSITORY or value["revision"] != REVISION:
        raise DiarizationError("unreviewed Community-1 revision")
    root = safe.path_value(value["root"])
    if not isinstance(value["files"], list) or len(value["files"]) != len(UPSTREAM_FILES):
        raise DiarizationError("complete pinned model mirror required")
    files, witnesses, seen, config = {}, {}, set(), None
    for row in value["files"]:
        safe.exact(row, {"relative_path", "sha256", "byte_count"}, "bundle file")
        relative = row["relative_path"]
        if not isinstance(relative, str) or relative not in UPSTREAM_FILES or relative in seen:
            raise DiarizationError("unknown, escaped or duplicate bundle file")
        seen.add(relative)
        size, blob, lfs = UPSTREAM_FILES[relative]
        artifact = {"path": str(root / relative), "sha256": row["sha256"]}
        if type(row["byte_count"]) is not int or row["byte_count"] != size:
            raise DiarizationError("upstream artifact size differs")
        body, witness = _read(artifact, size)
        if len(body) != size or (relative in WEIGHT_SHA256 and row["sha256"] != WEIGHT_SHA256[relative]):
            raise DiarizationError("unreviewed model weight bytes")
        git_body = (f"version https://git-lfs.github.com/spec/v1\noid sha256:{row['sha256']}\nsize {size}\n".encode()
                    if lfs else body)
        if _git_blob(git_body) != blob:
            raise DiarizationError("artifact differs from pinned upstream Git identity")
        files[relative], witnesses[relative] = {**artifact, "byte_count": size}, witness
        if relative == "config.yaml":
            config = _config(body)
    runtime, runtime_witness = _json(value["runtime"])
    _runtime_schema(runtime)
    _runtime_files_present(runtime)
    _read(value["license"], 1024**2)
    review, _ = _json(value["review_evidence"])
    _version(review, "himr_community1_bundle_review", {"repository", "revision", "license", "terms_accepted",
        "offline_runtime_reviewed", "reviewer", "reviewed_at", "bundle_files_sha256", "runtime", "license_snapshot"})
    if (review["repository"] != REPOSITORY or review["revision"] != REVISION or review["license"] != "CC-BY-4.0"
            or review["terms_accepted"] is not True or review["offline_runtime_reviewed"] is not True
            or not isinstance(review["reviewer"], str) or not 1 <= len(review["reviewer"]) <= 200
            or not isinstance(review["reviewed_at"], str) or not 1 <= len(review["reviewed_at"]) <= 80
            or review["bundle_files_sha256"] != hashlib.sha256(_canonical(value["files"])).hexdigest()
            or review["runtime"] != value["runtime"] or review["license_snapshot"] != value["license"]):
        raise DiarizationError("missing or mismatched explicit bundle/license/runtime review")
    return {"binding": copy.deepcopy(binding), "manifest_witness": manifest_witness,
            "repository": REPOSITORY, "revision": REVISION, "files": files, "witnesses": witnesses,
            "config": config, "runtime_binding": value["runtime"], "runtime_witness": runtime_witness,
            "runtime": runtime, "review_evidence": value["review_evidence"], "license": value["license"],
            "runtime_files_present_byte_count_checked": True, "runtime_files_sha256_reverified": False,
            "native_inference_verified": False}


def validate_request(request):
    _version(request, "himr_screened_diarization_worker_request", {"model_bundle", "audio_pcm", "recording_id",
        "media_sha256", "duration_ms", "device", "gpu_uuid", "threads", "resources", "speaker_parameters"})
    _binding(request["model_bundle"])
    _binding(request["audio_pcm"], sized=True)
    if not isinstance(request["recording_id"], str) or not safe.IDENTIFIER.fullmatch(request["recording_id"]):
        raise DiarizationError("invalid recording ID")
    if not isinstance(request["media_sha256"], str) or not safe.SHA.fullmatch(request["media_sha256"]):
        raise DiarizationError("invalid source media SHA-256")
    if request["device"] not in ("cpu", "cuda"):
        raise DiarizationError("device must be explicitly cpu or cuda")
    if (request["device"] == "cpu" and request["gpu_uuid"] is not None) or (request["device"] == "cuda" and
            (not isinstance(request["gpu_uuid"], str) or not re.fullmatch(
                r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", request["gpu_uuid"]))):
        raise DiarizationError("explicit matching GPU UUID required")
    safe.integer(request["threads"], 1, 16, "threads")
    resources = request["resources"]
    safe.exact(resources, {"max_duration_ms", "max_pcm_bytes", "max_waveform_bytes", "max_turns",
        "segmentation_batch_size", "embedding_batch_size", "cuda_memory_fraction"}, "worker resources")
    safe.integer(resources["max_duration_ms"], 1, MAX_DURATION_MS, "maximum duration")
    safe.integer(request["duration_ms"], 1, resources["max_duration_ms"], "audio duration")
    safe.integer(resources["max_pcm_bytes"], 32, MAX_DURATION_MS * 32, "maximum PCM bytes")
    safe.integer(resources["max_waveform_bytes"], 64, MAX_DURATION_MS * 64, "maximum waveform bytes")
    safe.integer(resources["max_turns"], 1, MAX_TURNS, "maximum turns")
    for name in ("segmentation_batch_size", "embedding_batch_size"):
        safe.integer(resources[name], 1, 16, name)
    fraction = resources["cuda_memory_fraction"]
    if type(fraction) not in (int, float) or not math.isfinite(fraction) or not .1 <= fraction <= .9:
        raise DiarizationError("invalid CUDA allocation fraction")
    size = request["duration_ms"] * 32
    if (request["audio_pcm"]["byte_count"] != size or size > resources["max_pcm_bytes"]
            or size * 2 > resources["max_waveform_bytes"]):
        raise DiarizationError("full PCM timeline or float32 waveform exceeds configured bound")
    parameters = request["speaker_parameters"]
    if not isinstance(parameters, dict) or set(parameters) - {"num_speakers", "min_speakers", "max_speakers"}:
        raise DiarizationError("unreviewed speaker parameter")
    for number in parameters.values():
        safe.integer(number, 1, 256, "speaker bound")
    if (("num_speakers" in parameters and len(parameters) != 1)
            or parameters.get("min_speakers", 1) > parameters.get("max_speakers", 256)):
        raise DiarizationError("inconsistent speaker parameters")
    return copy.deepcopy(request)


def validate_provenance(provenance, request, *, expected_implementation=None):
    """Parent-side proof validation; no ML imports and no inference."""
    request = validate_request(request)
    safe.exact(provenance, {"model_bundle", "model_revision", "model_files", "runtime", "audio_pcm",
        "source_media_sha256", "recording_id", "implementation", "device", "gpu", "threads", "resources",
        "speaker_parameters", "parameters", "checkpoint_load", "semantics"}, "engine provenance")
    for name, expected in (("model_bundle", request["model_bundle"]), ("model_revision", REVISION),
            ("audio_pcm", request["audio_pcm"]), ("source_media_sha256", request["media_sha256"]),
            ("recording_id", request["recording_id"]), ("device", request["device"]),
            ("threads", request["threads"]), ("resources", request["resources"]),
            ("speaker_parameters", request["speaker_parameters"]), ("semantics", SEMANTICS),
            ("checkpoint_load", "forced_weights_only_fixed_safe_globals_and_architectures")):
        if _canonical(provenance[name]) != _canonical(expected):
            raise DiarizationError("worker provenance differs from bound request: " + name)
    _binding(provenance["implementation"])
    expected_implementation = expected_implementation or {"path": str(Path(__file__).resolve()), "sha256": _self_hash()}
    _binding(expected_implementation)
    if provenance["implementation"] != expected_implementation:
        raise DiarizationError("worker implementation differs from parent pin")
    admitted = admit_bundle(request["model_bundle"])
    if provenance["model_files"] != admitted["files"] or provenance["parameters"] != admitted["config"]:
        raise DiarizationError("worker used different model files or parameters")
    runtime = provenance["runtime"]
    safe.exact(runtime, {"manifest", "python", "versions", "installed_files_sha256",
        "all_registered_runtime_files_and_wheels_verified"}, "worker runtime provenance")
    expected_runtime = {"manifest": admitted["runtime_binding"], "python": admitted["runtime"]["python"],
        "versions": {row["name"]: row["version"] for row in admitted["runtime"]["packages"]},
        "installed_files_sha256": hashlib.sha256(_canonical(admitted["runtime"]["installed_files"])).hexdigest(),
        "all_registered_runtime_files_and_wheels_verified": True}
    if _canonical(runtime) != _canonical(expected_runtime):
        raise DiarizationError("worker runtime differs from registered complete lock")
    gpu = provenance["gpu"]
    if request["device"] == "cpu":
        if gpu is not None:
            raise DiarizationError("CPU worker reported a GPU")
    else:
        safe.exact(gpu, {"uuid", "name", "total_memory", "cuda_runtime", "allocator_fraction"}, "GPU provenance")
        safe.integer(gpu["total_memory"], 256 * 1024**2, 1024**4, "GPU total memory")
        if (gpu["uuid"] != request["gpu_uuid"] or gpu["allocator_fraction"] != request["resources"]["cuda_memory_fraction"]
                or not isinstance(gpu["name"], str) or not 1 <= len(gpu["name"]) <= 200
                or not isinstance(gpu["cuda_runtime"], str) or not re.fullmatch(r"[0-9]+\.[0-9]+", gpu["cuda_runtime"])):
            raise DiarizationError("GPU provenance differs from explicit device request")
    return copy.deepcopy(provenance)


def _verify_runtime(admitted):
    runtime = admitted["runtime"]
    python = runtime["python"]
    if python["version"] != ".".join(map(str, sys.version_info[:3])) or Path(sys.executable).resolve() != Path(python["path"]):
        raise DiarizationError("worker is not the exact registered Python runtime")
    expected = {row["name"]: row["version"] for row in runtime["packages"]}
    actual, recorded = {}, set()
    for distribution in importlib.metadata.distributions():
        name = re.sub(r"[-_.]+", "-", distribution.metadata["Name"]).lower()
        if name in actual or not distribution.files:
            raise DiarizationError("duplicate or unregistered installed distribution")
        actual[name] = distribution.version
        for entry in distribution.files:
            path = Path(distribution.locate_file(entry)).resolve()
            if path.is_file():
                recorded.add(str(path))
    if actual != expected:
        raise DiarizationError("installed package versions differ from complete runtime lock")
    provided = {row["path"] for row in runtime["installed_files"]}
    if recorded - provided:
        raise DiarizationError("runtime lock omits installed distribution files")
    checks = [*runtime["installed_files"], *[row["wheel"] for row in runtime["packages"]],
              {key: python[key] for key in ("path", "sha256", "byte_count")}]
    deadline = time.monotonic() + 600
    witnesses = {}
    for row in checks:
        with safe.opened(row["path"]) as fd:
            actual_sha = (hashlib.sha256(b"").hexdigest() if safe.witness(fd)["st_size"] == 0 else
                          safe.hash_fd(fd, 32 * 1024**3, deadline))
            if safe.witness(fd)["st_size"] != row["byte_count"] or actual_sha != row["sha256"]:
                raise DiarizationError("runtime file or wheel differs from registered hash")
            witnesses[row["path"]] = safe.witness(fd)
    return {"manifest": admitted["runtime_binding"], "python": python, "versions": actual,
            "installed_files_sha256": hashlib.sha256(_canonical(runtime["installed_files"])).hexdigest(),
            "all_registered_runtime_files_and_wheels_verified": True, "_witnesses": witnesses}


def _offline_environment(request):
    # Worker-only process-global policy. No model import occurs before this call.
    for name in list(os.environ):
        if (name in {"HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "PYANNOTE_AUTH_TOKEN", "TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD"}
                or name.lower().endswith("_proxy")):
            os.environ.pop(name, None)
    os.environ.update(ENVIRONMENT)
    os.environ["CUDA_VISIBLE_DEVICES"] = request["gpu_uuid"] if request["device"] == "cuda" else ""
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[name] = str(request["threads"])
    safe.deny_internet()


def _load_model(role, body, torch, model_class, safe_globals):
    """No permissive loader fallback, even for the exact upstream hash."""
    with torch.serialization.safe_globals(safe_globals):
        loaded = torch.load(io.BytesIO(body), weights_only=True, map_location="cpu")
        try:
            architecture = loaded["pyannote.audio"]["architecture"]
            actual = (architecture["module"], architecture["class"])
        except (KeyError, TypeError) as error:
            raise DiarizationError("checkpoint architecture metadata missing") from error
        if actual != ARCHITECTURES[role]:
            raise DiarizationError("unreviewed checkpoint model architecture")
        if not isinstance(loaded.get("state_dict"), dict) or not loaded["state_dict"]:
            raise DiarizationError("checkpoint has no state dictionary")
        del loaded
        # Upstream requests weights_only=False; PyTorch's process-wide force flag
        # overrides that explicit argument. No model code is monkeypatched.
        if os.environ.get("TORCH_FORCE_WEIGHTS_ONLY_LOAD") != "1":
            raise DiarizationError("restricted checkpoint loading is not enforced")
        model = model_class.from_pretrained(io.BytesIO(body), map_location="cpu", strict=True, token=False)
        if model is None or (type(model).__module__, type(model).__name__) != actual:
            raise DiarizationError("loaded model class differs from checked architecture")
        model.eval()
        return model


def _annotations(output, duration_ms, maximum):
    result = {}
    for name, attribute in (("ordinary", "speaker_diarization"), ("exclusive", "exclusive_speaker_diarization")):
        annotation = getattr(output, attribute, None)
        if annotation is None or not callable(getattr(annotation, "itertracks", None)):
            raise DiarizationError("model omitted required diarization representation")
        rows = []
        for segment, _track, label in annotation.itertracks(yield_label=True):
            if len(rows) >= maximum:
                raise DiarizationError("model turn count exceeds bound")
            if (isinstance(segment.start, bool) or isinstance(segment.end, bool)
                    or not isinstance(segment.start, numbers.Real) or not isinstance(segment.end, numbers.Real)):
                raise DiarizationError("invalid model timestamp type")
            start, end = float(segment.start), float(segment.end)
            if (not math.isfinite(start) or not math.isfinite(end) or not 0 <= start < end <= duration_ms / 1000
                    or not isinstance(label, str) or not re.fullmatch(r"SPEAKER_[0-9]{2,6}", label)):
                raise DiarizationError("invalid, named, or out-of-bounds model turn")
            rows.append({"start": start, "end": end, "speaker": label})
        rows.sort(key=lambda row: (row["start"], row["end"], row["speaker"]))
        for previous, current in zip(rows, rows[1:]):
            if previous == current:
                raise DiarizationError("duplicate model turns")
            if name == "exclusive" and current["start"] < previous["end"]:
                raise DiarizationError("exclusive diarization contains overlap")
        result[name] = rows
    if {row["speaker"] for row in result["exclusive"]} - {row["speaker"] for row in result["ordinary"]}:
        raise DiarizationError("exclusive representation contains unknown speaker")
    return result


def _native_inference(request, admitted, pcm_fd):
    import numpy as np
    import torch
    from pyannote.audio import Model
    from pyannote.audio.core.task import Problem, Resolution, Specifications
    from pyannote.audio.core.plda import PLDA
    from pyannote.audio.pipelines import SpeakerDiarization
    from pyannote.audio.telemetry import set_telemetry_metrics
    set_telemetry_metrics(False)
    if tuple(int(v) for v in torch.__version__.split("+")[0].split(".")[:2]) < (2, 8):
        raise DiarizationError("PyTorch2.8 or newer is required for this reviewed API")
    torch.set_num_threads(request["threads"])
    if torch.get_num_interop_threads() != 1:
        torch.set_num_interop_threads(1)
    torch.set_float32_matmul_precision("highest")
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True
    torch.use_deterministic_algorithms(True)
    gpu = None
    if request["device"] == "cuda":
        if not torch.cuda.is_available() or torch.cuda.device_count() != 1:
            raise DiarizationError("exactly one explicitly masked CUDA device required")
        properties = torch.cuda.get_device_properties(0)
        uuid = str(getattr(properties, "uuid", ""))
        if uuid.lower() != request["gpu_uuid"].lower():
            raise DiarizationError("CUDA device UUID could not be verified")
        torch.cuda.set_per_process_memory_fraction(request["resources"]["cuda_memory_fraction"], 0)
        gpu = {"uuid": uuid, "name": str(properties.name), "total_memory": properties.total_memory,
               "cuda_runtime": torch.version.cuda, "allocator_fraction": request["resources"]["cuda_memory_fraction"]}
    models = {}
    for role in ("segmentation", "embedding"):
        ref = admitted["files"][role + "/pytorch_model.bin"]
        body, witness = _read({k: ref[k] for k in ("path", "sha256")}, ref["byte_count"])
        if witness != admitted["witnesses"][role + "/pytorch_model.bin"]:
            raise DiarizationError("model changed after admission")
        models[role] = _load_model(role, body, torch, Model, [Problem, Resolution, Specifications])
    # PLDA accepts file-like buffers through NumPy's np.load; vbx_setup uses the
    # default allow_pickle=False. Bytes are reverified before entering that code.
    arrays = []
    for name in ("plda/xvec_transform.npz", "plda/plda.npz"):
        ref = admitted["files"][name]
        body, witness = _read({k: ref[k] for k in ("path", "sha256")}, ref["byte_count"])
        if witness != admitted["witnesses"][name]:
            raise DiarizationError("PLDA changed after admission")
        with np.load(io.BytesIO(body), allow_pickle=False) as archive:
            for key in archive.files:
                array = archive[key]
                if array.dtype.hasobject or not np.isfinite(array).all():
                    raise DiarizationError("invalid PLDA array")
        arrays.append(io.BytesIO(body))
    plda = PLDA(*arrays)
    config = admitted["config"]
    pipeline = SpeakerDiarization(segmentation=models["segmentation"], embedding=models["embedding"], plda=plda,
        clustering="VBxClustering", embedding_exclude_overlap=config["pipeline"]["params"]["embedding_exclude_overlap"],
        segmentation_batch_size=request["resources"]["segmentation_batch_size"],
        embedding_batch_size=request["resources"]["embedding_batch_size"], token=False, legacy=False)
    pipeline.instantiate(config["params"])
    pipeline.to(torch.device("cuda:0" if request["device"] == "cuda" else "cpu"))
    # Retained FD avoids reopening an untrusted path. The one full CPU waveform
    # is float32; Community-1 owns its internal windowing and global clustering.
    with os.fdopen(os.dup(pcm_fd), "rb") as stream:
        samples = np.fromfile(stream, dtype="<i2", count=request["audio_pcm"]["byte_count"] // 2)
    if samples.size != request["duration_ms"] * 16:
        raise DiarizationError("PCM changed or has an incomplete recording timeline")
    waveform = torch.from_numpy(samples.astype(np.float32)).unsqueeze(0)
    waveform.div_(32768.0)
    del samples
    with torch.inference_mode(), torch.autocast(device_type=request["device"], enabled=False):
        output = pipeline({"waveform": waveform, "sample_rate": 16000, "uri": request["recording_id"]},
                          **request["speaker_parameters"])
    turns = _annotations(output, request["duration_ms"], request["resources"]["max_turns"])
    # Do not serialize, persist, or return speaker_embeddings.
    return turns, gpu


def run_worker(request):
    """One process, one full recording. Call only in the bounded child."""
    request = validate_request(request)
    implementation_hash = _self_hash()
    _offline_environment(request)
    admitted = admit_bundle(request["model_bundle"])
    runtime = _verify_runtime(admitted)
    runtime_witnesses = runtime.pop("_witnesses")
    audio = request["audio_pcm"]
    with safe.opened(audio["path"]) as pcm:
        witness = safe.witness(pcm)
        if witness["st_size"] != audio["byte_count"] or safe.hash_fd(pcm, audio["byte_count"], time.monotonic() + 600) != audio["sha256"]:
            raise DiarizationError("normalized PCM differs from bound recording")
        with redirect_stdout(sys.stderr):
            turns, gpu = _native_inference(request, admitted, pcm)
        with safe.opened(audio["path"]) as current:
            if safe.witness(pcm) != witness or safe.witness(current) != witness:
                raise DiarizationError("normalized PCM changed during inference")
    # Recheck all provenance sources, not just bytes loaded into model memory.
    if admit_bundle(request["model_bundle"]) != admitted:
        raise DiarizationError("model bundle changed during inference")
    for path, before in runtime_witnesses.items():
        with safe.opened(path) as current:
            if safe.witness(current) != before:
                raise DiarizationError("registered runtime changed during inference")
    implementation, _ = _read({"path": str(Path(__file__).resolve()),
        "sha256": implementation_hash}, 1024**2)
    provenance = {"model_bundle": request["model_bundle"], "model_revision": REVISION,
        "model_files": admitted["files"], "runtime": runtime, "audio_pcm": audio,
        "source_media_sha256": request["media_sha256"], "recording_id": request["recording_id"],
        "implementation": {"path": str(Path(__file__).resolve()), "sha256": hashlib.sha256(implementation).hexdigest()},
        "device": request["device"], "gpu": gpu, "threads": request["threads"],
        "resources": request["resources"], "speaker_parameters": request["speaker_parameters"],
        "parameters": admitted["config"], "checkpoint_load": "forced_weights_only_fixed_safe_globals_and_architectures",
        "semantics": dict(SEMANTICS)}
    result = {"kind": "himr_screened_diarization_engine_output", "schema_version": 1, **turns, "provenance": provenance}
    if len(_canonical(result)) > MAX_JSON:
        raise DiarizationError("worker result exceeds bounded JSON size")
    return result


def _self_hash():
    with safe.opened(str(Path(__file__).resolve())) as fd:
        return safe.hash_fd(fd, 1024**2, time.monotonic() + 30)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        request, _ = _json({"path": args.request, "sha256": args.expected_sha256})
        result = run_worker(request)
        sys.stdout.buffer.write(_canonical(result) + b"\n")
        return 0
    except (DiarizationError, OSError, ImportError, RuntimeError, ValueError, KeyError, TypeError) as error:
        # Exception text can contain checkpoint values; stderr is deliberately
        # generic and never reveals file contents, credentials or embeddings.
        sys.stderr.write(f"DiarizationEngineError: {type(error).__name__}; offline worker failed closed\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
