"""Checksummed, append-only registration of exact local model weights."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .asr_result_importer import (
    _absolute_observed_path,
    _exact_keys,
    _identifier,
    _integer,
    _object,
    _sha256,
    _stable_read,
    _string,
    _timestamp,
    _verify_hash,
)
from .db import transaction, utc_now
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError


MAX_MANIFEST_BYTES = 4 * 1024 * 1024


def _read_manifest(path_value: str | Path) -> tuple[dict[str, Any], str, Path]:
    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    body = _stable_read(path, "model registry manifest", maximum_bytes=MAX_MANIFEST_BYTES)
    try:
        raw = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"model registry manifest is invalid JSON: {error}") from error
    manifest = _object(raw, "model registry manifest")
    digest = sha256_bytes(canonical_json(manifest).encode("utf-8"))
    return manifest, digest, path.resolve()


def validate_model_registry_manifest(
    manifest_path: str | Path, *, verify_files: bool = True
) -> dict[str, Any]:
    """Validate a private manifest and optionally rehash all current weight files."""

    manifest, digest, path = _read_manifest(manifest_path)
    _exact_keys(
        manifest,
        "model registry manifest",
        {"schema_version", "manifest_id", "created_at", "registered_by", "basis", "models"},
    )
    if manifest["schema_version"] != 1:
        raise ResultImportError("model registry manifest.schema_version must equal 1")
    manifest_id = _identifier(manifest["manifest_id"], "model registry manifest.manifest_id")
    created_at = _timestamp(manifest["created_at"], "model registry manifest.created_at")
    if manifest["created_at"] != created_at:
        raise ResultImportError("model registry manifest.created_at must be canonical UTC")
    registered_by = _string(
        manifest["registered_by"], "model registry manifest.registered_by", maximum=500
    )
    basis = _string(manifest["basis"], "model registry manifest.basis", maximum=4_000)
    models_raw = manifest["models"]
    if not isinstance(models_raw, list) or not 1 <= len(models_raw) <= 32:
        raise ResultImportError("model registry manifest.models must contain 1 to 32 models")
    model_ids: set[str] = set()
    models: list[dict[str, Any]] = []
    for index, model_value in enumerate(models_raw):
        label = f"model registry manifest.models[{index}]"
        model = _object(model_value, label)
        _exact_keys(
            model,
            label,
            {
                "model_id",
                "task",
                "name",
                "version",
                "weights_path",
                "weights_sha256",
                "weights_byte_count",
                "license_label",
                "configuration_json",
            },
        )
        model_id = _identifier(model["model_id"], f"{label}.model_id")
        if model_id in model_ids:
            raise ResultImportError("model registry manifest contains duplicate model IDs")
        model_ids.add(model_id)
        task = _string(model["task"], f"{label}.task", maximum=200)
        name = _string(model["name"], f"{label}.name", maximum=1_000)
        version = _string(model["version"], f"{label}.version", maximum=2_000)
        digest_value = _sha256(model["weights_sha256"], f"{label}.weights_sha256")
        byte_count = _integer(
            model["weights_byte_count"], f"{label}.weights_byte_count", minimum=1
        )
        license_label = _string(
            model["license_label"], f"{label}.license_label", maximum=2_000
        )
        configuration = _object(model["configuration_json"], f"{label}.configuration_json")
        source = _string(
            configuration.get("source"), f"{label}.configuration_json.source", maximum=4_000
        )
        weights_path = _absolute_observed_path(model["weights_path"], f"{label}.weights_path")
        if verify_files:
            _verify_hash(weights_path, digest_value, byte_count, f"registered model {model_id}")
        models.append(
            {
                "model_id": model_id,
                "task": task,
                "name": name,
                "version": version,
                "weights_path": str(weights_path),
                "weights_uri": weights_path.as_uri(),
                "weights_sha256": digest_value,
                "weights_byte_count": byte_count,
                "license_label": license_label,
                "configuration_json": configuration,
                "source": source,
            }
        )
    return {
        "schema_version": 1,
        "manifest_id": manifest_id,
        "created_at": created_at,
        "registered_by": registered_by,
        "basis": basis,
        "models": models,
        "model_count": len(models),
        "input_sha256": digest,
        "manifest_path": str(path),
    }


def _model_snapshot(model: dict[str, Any]) -> dict[str, Any]:
    return {
        "model_id": model["model_id"],
        "task": model["task"],
        "name": model["name"],
        "version": model["version"],
        "weights_uri": model["weights_uri"],
        "weights_sha256": model["weights_sha256"],
        "weights_byte_count": model["weights_byte_count"],
        "license_label": model["license_label"],
        "configuration_json": model["configuration_json"],
    }


def import_model_registry_manifest(
    connection: sqlite3.Connection, manifest_path: str | Path
) -> dict[str, Any]:
    """Rehash and atomically register every model in one private manifest."""

    manifest = validate_model_registry_manifest(manifest_path, verify_files=True)
    manifest_id = manifest["manifest_id"]
    digest = manifest["input_sha256"]
    with transaction(connection):
        existing_manifest = connection.execute(
            """
            SELECT input_sha256, schema_version, manifest_created_at, imported_at,
                   registered_by, basis, model_count
            FROM model_registry_manifest_imports WHERE manifest_id = ?
            """,
            (manifest_id,),
        ).fetchone()
        imported_at = existing_manifest["imported_at"] if existing_manifest else utc_now()
        if existing_manifest is not None:
            expected = {
                "input_sha256": digest,
                "schema_version": 1,
                "manifest_created_at": manifest["created_at"],
                "registered_by": manifest["registered_by"],
                "basis": manifest["basis"],
                "model_count": len(manifest["models"]),
            }
            if any(existing_manifest[key] != value for key, value in expected.items()):
                raise ResultImportError("model registry manifest_id already has different data")
        else:
            digest_collision = connection.execute(
                "SELECT manifest_id FROM model_registry_manifest_imports WHERE input_sha256 = ?",
                (digest,),
            ).fetchone()
            if digest_collision is not None:
                raise ResultImportError("model registry manifest bytes already use another manifest_id")
            connection.execute(
                """
                INSERT INTO model_registry_manifest_imports(
                    manifest_id, input_sha256, schema_version, manifest_created_at,
                    imported_at, registered_by, basis, model_count
                ) VALUES(?, ?, 1, ?, ?, ?, ?, ?)
                """,
                (
                    manifest_id,
                    digest,
                    manifest["created_at"],
                    imported_at,
                    manifest["registered_by"],
                    manifest["basis"],
                    len(manifest["models"]),
                ),
            )

        for ordinal, model in enumerate(manifest["models"]):
            existing_model = connection.execute(
                """
                SELECT task, name, version, weights_sha256, license_label, configuration_json
                FROM models WHERE model_id = ?
                """,
                (model["model_id"],),
            ).fetchone()
            expected_model = {
                "task": model["task"],
                "name": model["name"],
                "version": model["version"],
                "weights_sha256": model["weights_sha256"],
                "license_label": model["license_label"],
                "configuration_json": canonical_json(model["configuration_json"]),
            }
            if existing_model is not None:
                if any(existing_model[key] != value for key, value in expected_model.items()):
                    raise ResultImportError(
                        f"registered model {model['model_id']} already has different data"
                    )
            else:
                natural_collision = connection.execute(
                    """
                    SELECT model_id FROM models
                    WHERE task = ? AND name = ? AND version = ? AND weights_sha256 = ?
                    """,
                    (
                        model["task"], model["name"], model["version"], model["weights_sha256"]
                    ),
                ).fetchone()
                if natural_collision is not None:
                    raise ResultImportError("model provenance tuple already uses another model_id")
                connection.execute(
                    """
                    INSERT INTO models(
                        model_id, task, name, version, weights_sha256,
                        license_label, configuration_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        model["model_id"],
                        model["task"],
                        model["name"],
                        model["version"],
                        model["weights_sha256"],
                        model["license_label"],
                        expected_model["configuration_json"],
                    ),
                )

            snapshot_text = canonical_json(_model_snapshot(model))
            existing_link = connection.execute(
                """
                SELECT model_id, model_snapshot_json
                FROM model_registry_manifest_models
                WHERE manifest_id = ? AND ordinal = ?
                """,
                (manifest_id, ordinal),
            ).fetchone()
            if existing_link is not None:
                if (
                    existing_link["model_id"] != model["model_id"]
                    or existing_link["model_snapshot_json"] != snapshot_text
                ):
                    raise ResultImportError("model registry manifest link already has different data")
            else:
                connection.execute(
                    """
                    INSERT INTO model_registry_manifest_models(
                        manifest_id, ordinal, model_id, model_snapshot_json
                    ) VALUES(?, ?, ?, ?)
                    """,
                    (manifest_id, ordinal, model["model_id"], snapshot_text),
                )
    return {
        "manifest_id": manifest_id,
        "input_sha256": digest,
        "imported_at": imported_at,
        "model_count": len(manifest["models"]),
        "model_ids": [model["model_id"] for model in manifest["models"]],
    }
