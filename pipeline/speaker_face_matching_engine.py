"""Offline LR-ASD adapter for anonymous, recording-local active-speaker evidence.

One bounded video clip and its simultaneous audio enter each worker. YuNet
detects faces, shot-local geometry tracks them, and LR-ASD scores usable tracks.
No downloads, training, recognition, embeddings, or model execution on import.
The parent owns preparation, proof replay, network-denied child execution, cgroup
memory limits, wall time, cancellation, and retention. This is an unvalidated
adapter, not an accuracy claim. Raw positive-class logits are not probabilities.

Reviewed upstream: Junhua-Liao/LR-ASD commit
1b6dcd2d8fc2895683de6508ec6294ec47d388ca, model/{Model,Encoder,Classifier}.py,
loss.py, dataLoader.py, and Columbia_test.py. Only the four fixed model modules
are executed. The training/CLI wrappers and their unsafe pickle loader are not.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, redirect_stdout, ExitStack
import copy
import hashlib
import io
import math
import os
from pathlib import Path
import re
import sys
import time
import types

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import speaker_screen as safe
from pipeline import screened_diarization_engine as common
from pipeline import speaker_face_matching_core as core

MatchingError = safe.ScreenError
REPOSITORY = "Junhua-Liao/LR-ASD"
REVISION = "1b6dcd2d8fc2895683de6508ec6294ec47d388ca"
MAX_FRAMES = 125
MAX_JSON = 32 * 1024**2
WEIGHT = "weight/pretrain_AVA.model"
YUNET_SHA256 = "8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4"
YUNET_BYTES = 232589
# Git blob identities and sizes read from the pinned GitHub tree. No weights
# were downloaded or deserialized to implement this adapter.
UPSTREAM_FILES = {
    "ASD.py": (4739, "1dd87f0197ee11640e3d2b217bba43390de7546e"),
    "Columbia_test.py": (21575, "27c6408eebe07326adc7a5b3cd38151fe4a328ba"),
    "LICENSE": (1068, "13c158de9cd708279994fc32817e5df7cd2b84dd"),
    "README.md": (4098, "d8ca7f9f1a185641ed79ce30f332cd181c5bfd52"),
    "dataLoader.py": (6410, "dde2b95b031ba883d50f99e5d9456cec13e7dbc9"),
    "loss.py": (1125, "a7ffcc66ef24b4713f8b018df27055c5e1760f44"),
    "model/Classifier.py": (1645, "9ebc2bdfaec3aa202375225f0fabd1e8f341fb3b"),
    "model/Encoder.py": (5408, "252e6a9b918d1a08bc950766ae1f87561d43d817"),
    "model/Model.py": (1417, "0a201474d82ca8b8efdb14470d55dcf4b7216f1f"),
    WEIGHT: (3426337, "d724be582f6d34f1b099657235dedafa0668fd82"),
}
MODULES = ("model.Classifier", "model.Encoder", "model.Model", "loss")
PREPROCESSING = {
    "audio": "signed_int16_little_endian_mono_16000_hz_unscaled",
    "video": "uint8_bgr_640x360_25fps_to_shot_local_gray_112x112_face_crops",
    "mfcc": {"implementation": "python-speech-features", "version": "0.6",
             "numcep": 13, "winlen": .025, "winstep": .010, "nfilt": 26,
             "nfft": 512, "lowfreq": 0, "highfreq": None, "preemph": .97,
             "ceplifter": 22, "appendEnergy": True, "window": "rectangular",
             "frames_per_video_frame": 4, "alignment": "upstream_val_wrap_shortage_then_truncate"},
    "video_normalization": "upstream_model:(x/255-0.4161)/0.1688",
    "context": "one_complete_bounded_track_no_multiscale_ensemble_no_smoothing",
}
SEMANTICS = {
    "score_type": "raw_positive_class_logit", "calibrated_probability": False,
    "network_denied": True, "telemetry_disabled": True,
    "face_embeddings_exported": False, "voice_embeddings_exported": False,
    "person_identity_claimed": False, "cross_recording_identity_linking": False,
    "offscreen_speaker_identification": False, "synchronization_estimated": False,
    "sync_evidence_scope": "container_timestamp_alignment_only_not_lip_sync",
    "occlusion_detector_present": False,
    "quality_gate_passed": False, "publication_authority": False,
    "float32": True, "tf32": False,
}
_canonical = common._canonical
_binding = common._binding
_read = common._read
_json = common._json
_version = common._version


def _runtime_schema(value):
    _version(value, "himr_lrasd_runtime", {"python", "packages", "installed_files"})
    python = value["python"]
    safe.exact(python, {"path", "sha256", "byte_count", "version"}, "runtime Python")
    _binding({key: python[key] for key in ("path", "sha256", "byte_count")}, sized=True)
    if not isinstance(python["version"], str) or not re.fullmatch(r"3\.[0-9]+\.[0-9]+", python["version"]):
        raise MatchingError("exact Python version required")
    packages, files = value["packages"], value["installed_files"]
    if not isinstance(packages, list) or not 1 <= len(packages) <= 512:
        raise MatchingError("bounded complete package inventory required")
    if not isinstance(files, list) or not 1 <= len(files) <= 100000:
        raise MatchingError("bounded complete installed file inventory required")
    names, paths = set(), set()
    for row in packages:
        safe.exact(row, {"name", "version", "wheel"}, "runtime package")
        name, version = row["name"], row["version"]
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", name) or name in names:
            raise MatchingError("duplicate or noncanonical package name")
        names.add(name)
        if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.+_-]{0,79}", version):
            raise MatchingError("exact package version required")
        _binding(row["wheel"], sized=True)
        if name == "python-speech-features" and version != "0.6":
            raise MatchingError("unreviewed MFCC implementation")
        if name == "torch" and (not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+(?:\+[^ ]+)?$", version)
                or tuple(map(int, version.split("+")[0].split(".")[:2])) < (2, 6)):
            raise MatchingError("PyTorch 2.6 or newer required for restricted loading")
    if not {"torch", "numpy", "scipy", "python-speech-features", "opencv-python-headless"} <= names:
        raise MatchingError("required registered runtime packages missing")
    if names & {"opencv-python", "opencv-contrib-python", "opencv-contrib-python-headless"}:
        raise MatchingError("ambiguous OpenCV distributions are not admitted")
    for row in files:
        _binding(row, sized=True, allow_empty=True)
        if row["path"] in paths:
            raise MatchingError("duplicate installed file")
        paths.add(row["path"])
    if sum(row["byte_count"] for row in files) > 32 * 1024**3:
        raise MatchingError("runtime inventory exceeds byte budget")
    return value


def admit_bundle(binding):
    """Read-only file admission. It does not import torch or prove inference."""
    value, manifest_witness = _json(binding)
    _version(value, "himr_lrasd_bundle", {"repository", "revision", "root", "files", "runtime", "license", "review_evidence", "yunet"})
    if value["repository"] != REPOSITORY or value["revision"] != REVISION:
        raise MatchingError("unreviewed LR-ASD repository/revision")
    root = safe.path_value(value["root"])
    if not isinstance(value["files"], list) or len(value["files"]) != len(UPSTREAM_FILES):
        raise MatchingError("complete pinned adapter model mirror required")
    files, witnesses = {}, {}
    for row in value["files"]:
        safe.exact(row, {"relative_path", "sha256", "byte_count"}, "bundle file")
        name = row["relative_path"]
        if not isinstance(name, str) or name not in UPSTREAM_FILES or name in files:
            raise MatchingError("unreviewed, escaped, or duplicate model file")
        size, git_blob = UPSTREAM_FILES[name]
        if type(row["byte_count"]) is not int or row["byte_count"] != size:
            raise MatchingError("model file size differs from upstream pin")
        ref = {"path": str(root / name), "sha256": row["sha256"]}
        body, witness = _read(ref, size)
        if len(body) != size or common._git_blob(body) != git_blob:
            raise MatchingError("model bytes differ from pinned upstream Git identity")
        files[name], witnesses[name] = {**ref, "byte_count": size}, witness
    license_ref = {key: files["LICENSE"][key] for key in ("path", "sha256")}
    if value["license"] != license_ref:
        raise MatchingError("license must bind the pinned upstream LICENSE")
    _binding(value["yunet"], sized=True)
    if value["yunet"]["byte_count"] != YUNET_BYTES or value["yunet"]["sha256"] != YUNET_SHA256:
        raise MatchingError("unreviewed YuNet model artifact")
    _, yunet_witness = _read({key: value["yunet"][key] for key in ("path", "sha256")}, YUNET_BYTES)
    runtime, runtime_witness = _json(value["runtime"])
    _runtime_schema(runtime)
    common._runtime_files_present(runtime)
    review, _ = _json(value["review_evidence"])
    _version(review, "himr_lrasd_bundle_review", {"repository", "revision", "license", "license_reviewed",
        "offline_runtime_reviewed", "reviewer", "reviewed_at", "bundle_files_sha256", "runtime", "license_snapshot", "yunet"})
    if (review["repository"] != REPOSITORY or review["revision"] != REVISION or review["license"] != "MIT"
            or review["license_reviewed"] is not True or review["offline_runtime_reviewed"] is not True
            or not isinstance(review["reviewer"], str) or not 1 <= len(review["reviewer"].strip()) <= 200
            or not isinstance(review["reviewed_at"], str) or not 1 <= len(review["reviewed_at"].strip()) <= 80
            or review["bundle_files_sha256"] != hashlib.sha256(_canonical(value["files"])).hexdigest()
            or review["runtime"] != value["runtime"] or review["license_snapshot"] != value["license"]
            or review["yunet"] != value["yunet"]):
        raise MatchingError("explicit matching bundle/license/runtime review required")
    return {"binding": copy.deepcopy(binding), "manifest_witness": manifest_witness,
        "repository": REPOSITORY, "revision": REVISION, "files": files, "witnesses": witnesses,
        "runtime_binding": value["runtime"], "runtime_witness": runtime_witness, "runtime": runtime,
        "review_evidence": value["review_evidence"], "license": value["license"],
        "yunet": value["yunet"], "yunet_witness": yunet_witness,
        "runtime_files_present_byte_count_checked": True, "runtime_files_sha256_reverified": False,
        "native_inference_verified": False}


def validate_request(request):
    _version(request, "himr_speaker_face_matching_worker_request", {"model_bundle", "audio_pcm", "video_rgb",
        "clip", "decode_receipt", "device", "gpu_uuid", "threads", "resources"})
    _binding(request["model_bundle"])
    _binding(request["decode_receipt"])
    clip = core.validate_clip(request["clip"])
    frame_count = (clip["end_ms"] - clip["start_ms"]) // 40
    for name in ("audio_pcm", "video_rgb"):
        _binding(request[name], sized=True)
    paths = [request[name]["path"] for name in ("audio_pcm", "video_rgb", "decode_receipt")]
    if len(set(paths)) != len(paths):
        raise MatchingError("audio, video, and decode receipt must be distinct artifacts")
    safe.integer(request["threads"], 1, 16, "threads")
    if request["device"] not in ("cpu", "cuda"):
        raise MatchingError("explicit cpu or cuda device required")
    if ((request["device"] == "cpu" and request["gpu_uuid"] is not None) or
        (request["device"] == "cuda" and (not isinstance(request["gpu_uuid"], str) or not re.fullmatch(
            r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", request["gpu_uuid"])))):
        raise MatchingError("explicit matching GPU UUID required")
    resources = request["resources"]
    safe.exact(resources, {"max_frames", "max_tracks", "cuda_memory_fraction"}, "worker resources")
    safe.integer(resources["max_frames"], 50, MAX_FRAMES, "maximum frames")
    safe.integer(frame_count, 50, resources["max_frames"], "contiguous frames")
    safe.integer(resources["max_tracks"], 1, 16, "maximum tracks")
    fraction = resources["cuda_memory_fraction"]
    if type(fraction) not in (int, float) or not math.isfinite(fraction) or not .1 <= fraction <= .9:
        raise MatchingError("invalid CUDA allocation fraction")
    for name, per_frame in (("audio_pcm", 1280), ("video_rgb", 640 * 360 * 3)):
        size = frame_count * per_frame
        if request[name]["byte_count"] != size:
            raise MatchingError("audio/video timeline size differs or exceeds limit")
    return copy.deepcopy(request)


def _runtime_proof(admitted):
    runtime = admitted["runtime"]
    return {"manifest": admitted["runtime_binding"], "python": runtime["python"],
        "versions": {row["name"]: row["version"] for row in runtime["packages"]},
        "installed_files_sha256": hashlib.sha256(_canonical(runtime["installed_files"])).hexdigest(),
        "all_registered_runtime_files_and_wheels_verified": True}


def _decode_sync(receipt, request):
    from pipeline import speaker_face_matching_visual as visual
    clip = request["clip"]
    visual.validate_receipt(receipt, start_ms=clip["start_ms"], end_ms=clip["end_ms"])
    for field, expected in (("audio_sha256", request["audio_pcm"]["sha256"]),
            ("video_sha256", request["video_rgb"]["sha256"]),
            ("audio_bytes", request["audio_pcm"]["byte_count"]),
            ("video_bytes", request["video_rgb"]["byte_count"])):
        if type(receipt[field]) is not type(expected) or receipt[field] != expected:
            raise MatchingError("decode receipt differs from prepared audio/video binding")
    return {"state": "verified", "offset_ms": 0}


def _provenance(request, admitted, runtime, gpu, implementation):
    return {"request": copy.deepcopy(request), "model_revision": REVISION,
        "model_files": admitted["files"], "yunet": admitted["yunet"], "runtime": runtime, "gpu": gpu,
        "implementation": implementation, "preprocessing": copy.deepcopy(PREPROCESSING),
        "visual_implementation": _visual_binding(),
        "checkpoint_load": "weights_only_strict_complete_tensor_state_dict",
        "semantics": copy.deepcopy(SEMANTICS)}


def validate_provenance(value, request, *, expected_implementation=None):
    request = validate_request(request)
    safe.exact(value, {"request", "model_revision", "model_files", "yunet", "runtime", "gpu", "implementation", "visual_implementation",
        "preprocessing", "checkpoint_load", "semantics"}, "engine provenance")
    admitted = admit_bundle(request["model_bundle"])
    expected_implementation = expected_implementation or {"path": str(Path(__file__).resolve()), "sha256": _self_hash()}
    _binding(expected_implementation)
    gpu = value["gpu"]
    if request["device"] == "cpu":
        if gpu is not None:
            raise MatchingError("CPU worker reported GPU execution")
    else:
        safe.exact(gpu, {"uuid", "name", "total_memory", "cuda_runtime", "allocator_fraction"}, "GPU provenance")
        safe.integer(gpu["total_memory"], 256 * 1024**2, 1024**4, "GPU memory")
        if (gpu["uuid"] != request["gpu_uuid"] or gpu["allocator_fraction"] != request["resources"]["cuda_memory_fraction"]
                or not isinstance(gpu["name"], str) or not 1 <= len(gpu["name"]) <= 200
                or not isinstance(gpu["cuda_runtime"], str) or not re.fullmatch(r"[0-9]+\.[0-9]+", gpu["cuda_runtime"])):
            raise MatchingError("GPU proof differs from explicit request")
    expected = _provenance(request, admitted, _runtime_proof(admitted), gpu, expected_implementation)
    if _canonical(value) != _canonical(expected):
        raise MatchingError("engine provenance differs from exact bound request/model/runtime")
    return copy.deepcopy(value)


def validate_output(value, request, *, expected_implementation=None):
    request = validate_request(request)
    _version(value, "himr_speaker_face_matching_engine_output", {"clip_id", "observations", "provenance"})
    if value["clip_id"] != request["clip"]["clip_id"]:
        raise MatchingError("engine output refers to another clip")
    observations = core.validate_observations(value["observations"], request["clip"])
    expected_times = list(range(request["clip"]["start_ms"], request["clip"]["end_ms"], 40))
    if [frame["time_ms"] for frame in observations["frames"]] != expected_times:
        raise MatchingError("engine observations omit prepared video frames")
    tracks = {(frame["shot_id"], face["track_id"]) for frame in observations["frames"] for face in frame["faces"]}
    if len(tracks) > request["resources"]["max_tracks"]:
        raise MatchingError("engine output exceeds registered track budget")
    if len({frame["shot_id"] for frame in observations["frames"]}) > 1 and any(
            face["raw_logit"] is not None for frame in observations["frames"] for face in frame["faces"]):
        raise MatchingError("a shot-cut clip cannot contain scored tracks")
    receipt, _ = _json(request["decode_receipt"])
    if observations["av_sync"] != _decode_sync(receipt, request):
        raise MatchingError("output timestamp alignment differs from replayed decode receipt")
    for shot, track in tracks:
        rows = [(frame["time_ms"], face) for frame in observations["frames"] if frame["shot_id"] == shot
                for face in frame["faces"] if face["track_id"] == track]
        if any(face["raw_logit"] is not None for _, face in rows):
            if (len(rows) < 25 or any(face["raw_logit"] is None or not face["visible"] or face["occluded"]
                    or min(face["face_width_px"], face["face_height_px"]) < 64 for _, face in rows)
                    or [when for when, _ in rows] != list(range(rows[0][0], rows[-1][0] + 40, 40))):
                raise MatchingError("model scored a short, discontinuous, or unusable face track")
    validate_provenance(value["provenance"], request, expected_implementation=expected_implementation)
    if len(_canonical(value)) > MAX_JSON:
        raise MatchingError("engine output exceeds bounded JSON size")
    return copy.deepcopy(value)


@contextmanager
def _fixed_modules(admitted):
    """Execute only hash-verified source bytes, never add a model path to sys.path.

    In-memory package modules prevent a stray model/__init__.py or cached .pyc
    from executing. Existing conflicting module names fail closed.
    """
    names = ("model", *MODULES)
    if any(name in sys.modules for name in names):
        raise MatchingError("worker has conflicting pre-imported model namespace")
    created = []
    try:
        package = types.ModuleType("model")
        package.__path__ = []
        sys.modules["model"] = package
        created.append("model")
        for name in MODULES:
            relative = name.replace(".", "/") + ".py"
            ref = admitted["files"][relative]
            body, witness = _read({key: ref[key] for key in ("path", "sha256")}, ref["byte_count"])
            if witness != admitted["witnesses"][relative]:
                raise MatchingError("registered model source changed before import")
            module = types.ModuleType(name)
            module.__file__ = ref["path"]
            module.__package__ = name.rpartition(".")[0]
            sys.modules[name] = module
            created.append(name)
            exec(compile(body, ref["path"], "exec", dont_inherit=True), module.__dict__)
        yield sys.modules["model.Model"].ASD_Model, sys.modules["loss"].lossAV, sys.modules["loss"].lossV
    finally:
        for name in reversed(created):
            sys.modules.pop(name, None)


def _load_weights(torch, wrapper, body):
    if os.environ.get("TORCH_FORCE_WEIGHTS_ONLY_LOAD") != "1":
        raise MatchingError("restricted checkpoint loading must be enforced")
    state = torch.load(io.BytesIO(body), weights_only=True, map_location="cpu")
    expected = wrapper.state_dict()
    if not isinstance(state, dict) or not state or len(state) > 1024 or set(state) != set(expected):
        raise MatchingError("checkpoint must match complete registered model state")
    for key, tensor in state.items():
        target = expected[key]
        if (not isinstance(key, str) or not isinstance(tensor, torch.Tensor) or tensor.is_sparse
                or tensor.shape != target.shape or tensor.dtype != target.dtype
                or not torch.isfinite(tensor).all().item()):
            raise MatchingError("checkpoint tensor shape/type/value differs")
    wrapper.load_state_dict(state, strict=True)
    wrapper.eval()
    return wrapper


def _features(np, mfcc, samples, frame_count):
    # Preserve int16 amplitude like scipy.io.wavfile in the upstream loader.
    features = mfcc(samples, 16000, numcep=13, winlen=.025, winstep=.010,
        nfilt=26, nfft=512, lowfreq=0, highfreq=None, preemph=.97, ceplifter=22, appendEnergy=True)
    target = frame_count * 4
    if features.ndim != 2 or features.shape[1] != 13 or features.shape[0] not in (target - 1, target):
        raise MatchingError("MFCC timeline differs from fixed upstream recipe")
    if not np.isfinite(features).all():
        raise MatchingError("MFCC contains non-finite values")
    if features.shape[0] < target:
        features = np.pad(features, ((0, target - features.shape[0]), (0, 0)), mode="wrap")
    return features[:target].astype(np.float32)


def _observations(request, detected, scores, sync):
    from pipeline import speaker_face_matching_visual as visual
    clip = request["clip"]
    count = (clip["end_ms"] - clip["start_ms"]) // 40
    if len(detected["tracks"]) > request["resources"]["max_tracks"]:
        raise MatchingError("clip exceeds configured face-track budget")
    if detected["cuts"] and scores:
        raise MatchingError("shot-cut clip cannot be scored")
    frames = []
    shot_index = 0
    for index in range(count):
        if index in detected["cuts"]:
            shot_index += 1
        frames.append({"time_ms": clip["start_ms"] + index * 40,
            "shot_id": visual.shot_id(clip["clip_id"], shot_index), "faces": []})
    for track in detected["tracks"]:
        track_scores = scores.get(track["track_id"])
        if track_scores is not None and len(track_scores) != len(track["frame_indices"]):
            raise MatchingError("track model logits omit face frames")
        for offset, (index, box) in enumerate(zip(track["frame_indices"], track["boxes"])):
            if frames[index]["shot_id"] != track["shot_id"]:
                raise MatchingError("face track crosses a shot boundary")
            frames[index]["faces"].append({"track_id": track["track_id"],
                "raw_logit": None if track_scores is None else float(track_scores[offset]),
                "face_width_px": max(1, math.floor(box["width"])),
                "face_height_px": max(1, math.floor(box["height"])),
                "visible": True, "occluded": False})
    for frame in frames:
        frame["faces"].sort(key=lambda face: face["track_id"])
    return core.validate_observations({"frames": frames, "av_sync": sync}, clip)


def _usable_tracks(detected):
    # Keep unusable visible faces in observations as null-scored competitors.
    # In particular, dropping a border crop would wrongly let another face win.
    if detected["cuts"]:
        return []
    return [track for track in detected["tracks"] if track["crops"] is not None
        and len(track["frame_indices"]) >= 25
        and all(min(box["width"], box["height"]) >= 64 for box in track["boxes"])]


def _native_inference(request, admitted, pcm_fd, video_fd, sync):
    import numpy as np
    import torch
    import cv2
    from python_speech_features import mfcc
    from pipeline import speaker_face_matching_visual as visual

    torch.set_num_threads(request["threads"])
    if torch.get_num_interop_threads() != 1:
        torch.set_num_interop_threads(1)
    torch.set_float32_matmul_precision("highest")
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True
    torch.use_deterministic_algorithms(True)
    torch.manual_seed(0)
    gpu = None
    if request["device"] == "cuda":
        if not torch.cuda.is_available() or torch.cuda.device_count() != 1:
            raise MatchingError("exactly one explicitly masked CUDA device required")
        properties = torch.cuda.get_device_properties(0)
        uuid = str(getattr(properties, "uuid", ""))
        if uuid.lower() != request["gpu_uuid"].lower():
            raise MatchingError("CUDA device UUID could not be verified")
        torch.cuda.set_per_process_memory_fraction(request["resources"]["cuda_memory_fraction"], 0)
        gpu = {"uuid": request["gpu_uuid"], "name": str(properties.name), "total_memory": properties.total_memory,
            "cuda_runtime": torch.version.cuda, "allocator_fraction": request["resources"]["cuda_memory_fraction"]}
    clip = request["clip"]
    frames, samples = visual.prepare_arrays(os.pread(video_fd, request["video_rgb"]["byte_count"], 0),
        os.pread(pcm_fd, request["audio_pcm"]["byte_count"], 0), start_ms=clip["start_ms"], end_ms=clip["end_ms"], np=np)
    cv2.setNumThreads(1)
    cv2.ocl.setUseOpenCL(False)
    yunet = admitted["yunet"]
    detector = cv2.FaceDetectorYN_create(yunet["path"], "", (640, 360),
        visual.DETECTION_THRESHOLD, .3, 5000, cv2.dnn.DNN_BACKEND_OPENCV, cv2.dnn.DNN_TARGET_CPU)
    with safe.opened(yunet["path"]) as fd:
        if safe.witness(fd) != admitted["yunet_witness"]:
            raise MatchingError("YuNet weights changed while loading")
    detected = visual.detect_and_track(frames, clip_id=clip["clip_id"], start_ms=clip["start_ms"], detector=detector, np=np, cv2=cv2)
    if len(detected["tracks"]) > request["resources"]["max_tracks"]:
        raise MatchingError("clip exceeds configured face-track budget")
    usable = _usable_tracks(detected)
    if not usable:
        return _observations(request, detected, {}, sync), gpu
    device = torch.device("cuda:0" if request["device"] == "cuda" else "cpu")
    ref = admitted["files"][WEIGHT]
    body, witness = _read({key: ref[key] for key in ("path", "sha256")}, ref["byte_count"])
    if witness != admitted["witnesses"][WEIGHT]:
        raise MatchingError("registered model weights changed before inference")
    with _fixed_modules(admitted) as (model_class, av_class, visual_class):
        wrapper = torch.nn.Module()
        wrapper.model = model_class()
        wrapper.lossAV = av_class()
        wrapper.lossV = visual_class()
        _load_weights(torch, wrapper, body).to(device)
        all_scores = {}
        with torch.inference_mode(), torch.autocast(device_type=request["device"], enabled=False):
            for track in usable:
                indices = track["frame_indices"]
                if indices != list(range(indices[0], indices[-1] + 1)):
                    raise MatchingError("LR-ASD track must contain contiguous frames")
                track_samples = samples[indices[0] * 640:(indices[-1] + 1) * 640]
                features = _features(np, mfcc, track_samples, len(indices))
                input_a = torch.from_numpy(features).unsqueeze(0).to(device)
                input_v = torch.from_numpy(track["crops"].astype(np.float32)).unsqueeze(0).to(device)
                audio_features = wrapper.model.forward_audio_frontend(input_a)
                visual_features = wrapper.model.forward_visual_frontend(input_v)
                output = wrapper.model.forward_audio_visual_backend(audio_features, visual_features)
                scores = wrapper.lossAV(output, labels=None)
                if scores.shape != (len(indices),) or not np.isfinite(scores).all():
                    raise MatchingError("model returned invalid frame logits")
                all_scores[track["track_id"]] = [float(score) for score in scores]
        return _observations(request, detected, all_scores, sync), gpu


def run_worker(request):
    """Call only in the bounded, isolated child; never in the live screen process."""
    request = validate_request(request)
    implementation = {"path": str(Path(__file__).resolve()), "sha256": _self_hash()}
    visual_implementation = _visual_binding()
    common._offline_environment(request)
    admitted = admit_bundle(request["model_bundle"])
    runtime = common._verify_runtime(admitted)
    runtime_witnesses = runtime.pop("_witnesses")
    with ExitStack() as stack:
        descriptors, witnesses = {}, {}
        for name in ("audio_pcm", "video_rgb"):
            ref = request[name]
            fd = stack.enter_context(safe.opened(ref["path"]))
            before = safe.witness(fd)
            if before["st_size"] != ref["byte_count"] or safe.hash_fd(fd, ref["byte_count"], time.monotonic() + 60) != ref["sha256"]:
                raise MatchingError("prepared audio or video differs from bound input")
            descriptors[name], witnesses[name] = fd, before
        receipt, receipt_witness = _json(request["decode_receipt"])
        sync = _decode_sync(receipt, request)
        with redirect_stdout(sys.stderr):
            observations, gpu = _native_inference(request, admitted, descriptors["audio_pcm"], descriptors["video_rgb"], sync)
        for name, fd in descriptors.items():
            with safe.opened(request[name]["path"]) as current:
                if safe.witness(fd) != witnesses[name] or safe.witness(current) != witnesses[name]:
                    raise MatchingError("prepared input changed during inference")
        if _json(request["decode_receipt"]) != (receipt, receipt_witness):
            raise MatchingError("decode receipt changed during inference")
    if admit_bundle(request["model_bundle"]) != admitted:
        raise MatchingError("registered bundle changed during inference")
    for path, before in runtime_witnesses.items():
        with safe.opened(path) as fd:
            if safe.witness(fd) != before:
                raise MatchingError("registered runtime changed during inference")
    _read(implementation, 1024**2)
    if _visual_binding() != visual_implementation:
        raise MatchingError("visual implementation changed during inference")
    output = {"kind": "himr_speaker_face_matching_engine_output", "schema_version": 1,
        "clip_id": request["clip"]["clip_id"], "observations": observations,
        "provenance": _provenance(request, admitted, runtime, gpu, implementation)}
    return validate_output(output, request, expected_implementation=implementation)


def _self_hash():
    with safe.opened(str(Path(__file__).resolve())) as fd:
        return safe.hash_fd(fd, 1024**2, time.monotonic() + 30)


def _visual_binding():
    path = str(Path(__file__).resolve().with_name("speaker_face_matching_visual.py"))
    with safe.opened(path) as fd:
        return {"path": path, "sha256": safe.hash_fd(fd, 1024**2, time.monotonic() + 30)}


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
    except (MatchingError, OSError, ImportError, RuntimeError, ValueError, KeyError, TypeError) as error:
        sys.stderr.write(f"SpeakerFaceMatchingEngineError: {type(error).__name__}; offline worker failed closed\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
