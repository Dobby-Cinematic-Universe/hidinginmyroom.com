from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


EVIDENCE = load_module(
    "himr_gpu_admission_evidence_test_module",
    ROOT / "pipeline/gpu/gpu_admission_evidence.py",
)


class GPUAdmissionEvidenceTests(unittest.TestCase):
    def raw_and_gate(
        self, name: str, **metrics: object
    ) -> tuple[dict[str, object], dict[str, object]]:
        values = EVIDENCE.empty_metrics()
        values.update(metrics)
        raw = EVIDENCE.make_raw_evidence(
            {
                "kind": EVIDENCE.RAW_EVIDENCE_KIND,
                "schema_version": EVIDENCE.RAW_EVIDENCE_SCHEMA_VERSION,
                "implementation_version": EVIDENCE.RAW_EVIDENCE_IMPLEMENTATION_VERSION,
                "gate": name,
                "production_profile_identity_sha256": "1" * 64,
                "execution_image_identity_sha256": "2" * 64,
                "runtime_candidate_identity_sha256": "4" * 64,
                "reducer": EVIDENCE.REDUCER_REFERENCE,
                "observations": [EVIDENCE.observation_from_metrics(values)],
                "policy": EVIDENCE.RAW_POLICY,
            }
        )
        gate = EVIDENCE.make_gate(
            {
                "kind": EVIDENCE.KIND,
                "schema_version": EVIDENCE.SCHEMA_VERSION,
                "implementation_version": EVIDENCE.IMPLEMENTATION_VERSION,
                "gate": name,
                "status": "passed",
                "production_profile_identity_sha256": "1" * 64,
                "execution_image_identity_sha256": "2" * 64,
                "runtime_candidate_identity_sha256": "4" * 64,
                "raw_evidence": EVIDENCE.make_raw_reference(
                    "/private/evidence.json", raw
                ),
                "metrics": EVIDENCE.reduce_raw_evidence(raw),
                "policy": EVIDENCE.POLICY,
            }
        )
        return raw, gate

    def gate(self, name: str, **metrics: object) -> dict[str, object]:
        return self.raw_and_gate(name, **metrics)[1]

    def test_all_gate_minima_have_explicit_acceptance_paths(self) -> None:
        examples = {
            "accuracy": dict(
                case_count=12,
                repetitions=3,
                audio_seconds=3600.0,
                wer_absolute_regression=0.005,
                wer_relative_regression=0.03,
                term_recall_absolute_regression=0.005,
            ),
            "semantic_compatibility": dict(item_count=3, exact_output_match=True),
            "packed_30m": dict(audio_seconds=1800.0, p95_end_to_end_rtf=1 / 30),
            "packed_2h": dict(audio_seconds=7200.0, p95_end_to_end_rtf=1 / 30),
            "thermal_8h": dict(wall_seconds=28800.0),
            "batch_32": dict(item_count=32),
            "scheduler": dict(lock_contention_passed=True, lock_crash_release_passed=True),
            "crash_recovery": dict(crash_case_count=8, crash_recovery_failure_count=0),
            "launcher_isolation": dict(
                network_isolated=True,
                cold_storage_visible=False,
                writable_root_expansion_observed=False,
            ),
        }
        for name, metrics in examples.items():
            with self.subTest(gate=name):
                gate = self.gate(name, **metrics)
                self.assertEqual(EVIDENCE.validate_gate(gate), gate)

    def test_short_smokes_cannot_claim_long_soak_gates(self) -> None:
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "30-minute"):
            self.gate("packed_30m", audio_seconds=1799.9)
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "eight"):
            self.gate("thermal_8h", wall_seconds=28799.9)
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "32"):
            self.gate("batch_32", item_count=31)

    def test_throughput_resource_and_isolation_regressions_fail(self) -> None:
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "throughput"):
            self.gate("packed_2h", audio_seconds=7200, p95_end_to_end_rtf=0.04)
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "resource"):
            self.gate("thermal_8h", wall_seconds=28800, maximum_temperature_c=81)
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "isolation"):
            self.gate("launcher_isolation", network_isolated=True, cold_storage_visible=True)

    def test_forged_identity_and_unknown_metrics_fail(self) -> None:
        gate = self.gate("batch_32", item_count=32)
        gate["identity_sha256"] = "0" * 64
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "identity"):
            EVIDENCE.validate_gate(gate)
        values = EVIDENCE.empty_metrics()
        values["unbounded"] = True
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "unexpected"):
            EVIDENCE._normalize_metrics(values)

    def test_gate_aggregate_must_replay_exactly_from_typed_raw(self) -> None:
        raw, gate = self.raw_and_gate("batch_32", item_count=32)
        forged_core = {key: gate[key] for key in EVIDENCE.CORE_FIELDS}
        forged_core["metrics"] = dict(gate["metrics"], item_count=33)
        forged = EVIDENCE.make_gate(forged_core)
        self.assertEqual(EVIDENCE.validate_gate(forged), forged)
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "do not replay"):
            EVIDENCE.validate_gate_against_raw(forged, raw)

    def test_raw_lineage_is_independently_bound_to_gate(self) -> None:
        raw, gate = self.raw_and_gate("batch_32", item_count=32)
        changed_core = {key: gate[key] for key in EVIDENCE.CORE_FIELDS}
        changed_core["runtime_candidate_identity_sha256"] = "5" * 64
        changed = EVIDENCE.make_gate(changed_core)
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "lineage"):
            EVIDENCE.validate_gate_against_raw(changed, raw)

    def test_raw_envelope_is_closed_text_free_and_bounded(self) -> None:
        raw, _ = self.raw_and_gate("batch_32", item_count=32)
        core = {key: raw[key] for key in EVIDENCE.RAW_CORE_FIELDS}
        core["observations"] = [dict(raw["observations"][0], note="looks good")]
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "unexpected"):
            EVIDENCE.make_raw_evidence(core)
        core = {key: raw[key] for key in EVIDENCE.RAW_CORE_FIELDS}
        core["observations"] = []
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "between"):
            EVIDENCE.make_raw_evidence(core)

    def test_reducer_is_pinned_and_uses_deterministic_worst_case_rules(self) -> None:
        first = EVIDENCE.empty_observation()
        first.update(
            ordinal=0,
            item_count=1,
            audio_milliseconds=1_000,
            end_to_end_rtf_ppb=10_000_000,
            maximum_temperature_millicelsius=50_000,
            exact_output_match=True,
        )
        second = EVIDENCE.empty_observation()
        second.update(
            ordinal=1,
            item_count=2,
            audio_milliseconds=2_000,
            end_to_end_rtf_ppb=30_000_000,
            maximum_temperature_millicelsius=79_000,
            exact_output_match=True,
        )
        raw = EVIDENCE.make_raw_evidence(
            {
                "kind": EVIDENCE.RAW_EVIDENCE_KIND,
                "schema_version": EVIDENCE.RAW_EVIDENCE_SCHEMA_VERSION,
                "implementation_version": EVIDENCE.RAW_EVIDENCE_IMPLEMENTATION_VERSION,
                "gate": "batch_32",
                "production_profile_identity_sha256": "1" * 64,
                "execution_image_identity_sha256": "2" * 64,
                "runtime_candidate_identity_sha256": "4" * 64,
                "reducer": EVIDENCE.REDUCER_REFERENCE,
                "observations": [first, second],
                "policy": EVIDENCE.RAW_POLICY,
            }
        )
        reduced = EVIDENCE.reduce_raw_evidence(raw)
        self.assertEqual(reduced["item_count"], 3)
        self.assertEqual(reduced["audio_seconds"], 3.0)
        self.assertEqual(reduced["p95_end_to_end_rtf"], 0.03)
        self.assertEqual(reduced["maximum_temperature_c"], 79.0)
        self.assertEqual(
            EVIDENCE.REDUCER_IDENTITY_SHA256,
            EVIDENCE.sha256_bytes(EVIDENCE.canonical_bytes(EVIDENCE.REDUCER_DESCRIPTOR)),
        )

    def test_noncanonical_or_reducer_substituted_raw_body_fails(self) -> None:
        raw, gate = self.raw_and_gate("batch_32", item_count=32)
        pretty = json.dumps(raw, sort_keys=True, indent=2).encode("utf-8")
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "canonical"):
            EVIDENCE.validate_gate_against_raw(gate, raw, raw_body=pretty)
        core = {key: raw[key] for key in EVIDENCE.RAW_CORE_FIELDS}
        core["reducer"] = dict(EVIDENCE.REDUCER_REFERENCE, identity_sha256="0" * 64)
        with self.assertRaisesRegex(EVIDENCE.AdmissionEvidenceError, "header"):
            EVIDENCE.make_raw_evidence(core)


if __name__ == "__main__":
    unittest.main()
