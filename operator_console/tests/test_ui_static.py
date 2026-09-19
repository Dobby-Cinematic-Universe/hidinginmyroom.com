from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
UI = ROOT / "operator_console" / "ui"


class OperatorUIStaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (UI / "index.html").read_text(encoding="utf-8")
        cls.javascript = (UI / "app.js").read_text(encoding="utf-8")
        cls.styles = (UI / "styles.css").read_text(encoding="utf-8")

    def run_browser_helpers(self, body: str):
        script = r'''
const fs = require("node:fs");
const vm = require("node:vm");
function element() {
  return {children: [], dataset: {}, hidden: false, textContent: "", value: 0,
    append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; },
    removeAttribute() {}, setAttribute() {},
  };
}
const context = {document: {addEventListener() {}, createElement: element}};
vm.createContext(context);
const appSource = fs.readFileSync("operator_console/ui/app.js", "utf8").replace(
  /\n\}\)\(\);\s*$/,
  "\nglobalThis.uiTest = {normalizeLongform, longformSnapshot, renderLongform, telemetrySources, deriveProgress, harvesterLifecycleMetrics, validateState, app, elements};\n})();"
);
vm.runInContext(appSource, context);
const ui = context.uiTest;
const base = {
  schema_version: 1, state: "available", updated_at: new Date().toISOString(),
  lifecycle: "running", basis: "cached_companion_status_completed_recording_jobs",
  counts: {completed_recordings: 3, discovered_recordings: 10, remaining_discovered_recordings: 7,
    unprepared_recordings: 2, preprocessed_recordings: 2, prepared_recordings: 2, incomplete_recordings: 1,
    cold_candidates: 9, queue_candidates: 1, expected_cold_backlog: 19, cold_candidates_not_discovered: 10,
    active_recordings: 1},
  completion_percent: 30, last_error: null, diagnostic: null,
};
const copy = () => JSON.parse(JSON.stringify(base));
ui.app.state = {autonomy: {actual_state: "running", longform: base}, jobs: [], actions: []};
for (const id of ["longform-age", "longform-detail", "longform-progress-section", "longform-progress",
  "longform-counts", "longform-lifecycle", "longform-error"]) ui.elements[id] = element();
'''
        result = subprocess.run(
            ["node", "--input-type=commonjs", "--eval", script + body],
            cwd=ROOT, check=False, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_longform_has_a_dedicated_discovered_only_panel(self) -> None:
        for element_id in ("longform-title", "longform-counts", "longform-age", "longform-progress", "longform-detail"):
            self.assertIn(f'id="{element_id}"', self.html)
        self.assertIn("of discovered recordings", self.html)
        self.assertIn("not the full campaign", self.html)
        self.assertIn("ASR complete (ordinary queue)", self.html + self.javascript)
        result = self.run_browser_helpers('''
ui.renderLongform();
console.log(JSON.stringify({
  metrics: ui.elements["longform-counts"].children.map(row => row.children.map(cell => cell.textContent)),
  hidden: ui.elements["longform-progress-section"].hidden,
  percent: ui.elements["longform-progress"].value,
  detail: ui.elements["longform-detail"].textContent,
}));
''')
        self.assertEqual(result["metrics"], [
            ["Long-form ASR complete", "3"], ["Discovered recordings", "10"],
            ["Remaining discovered", "7"], ["Cold not yet discovered", "10"],
            ["Unprepared", "2"], ["Preprocessed", "2"],
            ["Prepared", "2"], ["Incomplete", "1"], ["Active recordings", "1"],
        ])
        self.assertFalse(result["hidden"])
        self.assertEqual(result["percent"], 30)
        self.assertIn("3 of 10 discovered recordings", result["detail"])

    def test_longform_rejects_invalid_or_inconsistent_counts_without_failing_controls(self) -> None:
        result = self.run_browser_helpers('''
const variants = [
  x => { x.counts.completed_recordings = -1; },
  x => { x.counts.completed_recordings = "3"; },
  x => { x.counts.discovered_recordings = 1.5; },
  x => { x.counts.discovered_recordings = Number.MAX_SAFE_INTEGER + 1; },
  x => { x.counts.unprepared_recordings = null; },
  x => { x.counts.active_recordings = -1; },
  x => { x.counts.active_recordings = 2; },
  x => { x.counts.prepared_recordings = 3; },
  x => { x.counts.cold_candidates = 8; },
  x => { x.counts.queue_candidates = 2; },
  x => { x.counts.expected_cold_backlog = 20; },
  x => { x.counts.cold_candidates_not_discovered = 11; },
  x => { x.counts.completed_recordings = 11; },
  x => { x.counts.remaining_discovered_recordings = 8; },
  x => { x.completion_percent = Infinity; },
  x => { x.completion_percent = NaN; },
  x => { x.completion_percent = 90; },
  x => { x.updated_at = "yesterday"; },
  x => { x.schema_version = 2; },
  x => { x.basis = "full_campaign"; },
  x => { x.state = "completed"; },
];
const output = variants.map(change => {
  const value = copy(); change(value);
  const normalized = ui.normalizeLongform(value);
  const state = ui.validateState({csrf_token: "safe-token", revision: 1, profile_set_sha256: "a".repeat(64),
    profiles: [], actions: [], jobs: [], capacity: {global_limit: 1, active_count: 0, resources: {}},
    autonomy: {actual_state: "running", longform: value}});
  ui.app.state = state;
  ui.renderLongform();
  return [normalized.state, normalized.counts, state.revision,
    ui.elements["longform-counts"].hidden, ui.elements["longform-progress-section"].hidden];
});
console.log(JSON.stringify(output));
''')
        self.assertEqual(result, [["unavailable", None, 1, True, True]] * 21)

    def test_longform_missing_unregistered_unavailable_and_zero_are_distinct(self) -> None:
        result = self.run_browser_helpers('''
const zero = copy();
for (const key of Object.keys(zero.counts)) zero.counts[key] = 0;
zero.completion_percent = null;
const variants = [undefined, {...copy(), state: "not_registered", counts: null, completion_percent: null},
  {...copy(), state: "unavailable", counts: null, completion_percent: null}, zero];
console.log(JSON.stringify(variants.map(value => {
  ui.app.state.autonomy.longform = value;
  ui.renderLongform();
  return {state: ui.normalizeLongform(value).state, detail: ui.elements["longform-detail"].textContent,
    countRows: ui.elements["longform-counts"].children.length,
    progressHidden: ui.elements["longform-progress-section"].hidden};
})));
''')
        self.assertEqual([item["state"] for item in result], ["not_reported", "not_registered", "unavailable", "available"])
        self.assertEqual([item["countRows"] for item in result], [0, 0, 0, 9])
        self.assertTrue(all(item["progressHidden"] for item in result))
        self.assertIn("No long-form recordings have been discovered", result[-1]["detail"])

    def test_longform_stale_counts_are_explicitly_cached_and_progress_is_hidden(self) -> None:
        result = self.run_browser_helpers('''
const stale = copy(); stale.updated_at = new Date(Date.now() - 600_000).toISOString();
ui.app.state.autonomy.longform = stale;
ui.renderLongform();
const now = Date.parse(base.updated_at);
console.log(JSON.stringify({
  stale: ui.longformSnapshot(stale).freshness,
  badge: ui.elements["longform-age"].textContent,
  detail: ui.elements["longform-detail"].textContent,
  countRows: ui.elements["longform-counts"].children.length,
  progressHidden: ui.elements["longform-progress-section"].hidden,
  ageUnknown: ui.longformSnapshot({...copy(), updated_at: null}, now).freshness,
  future: ui.longformSnapshot({...copy(), updated_at: new Date(now + 120_000).toISOString()}, now).freshness,
  boundary: ui.longformSnapshot(base, now + 300_000).freshness,
  pastBoundary: ui.longformSnapshot(base, now + 300_001).freshness,
}));
''')
        self.assertEqual(result["stale"], "stale")
        self.assertIn("Stale", result["badge"])
        self.assertIn("Last cached snapshot only", result["detail"])
        self.assertEqual(result["countRows"], 9)
        self.assertTrue(result["progressHidden"])
        self.assertEqual([result[key] for key in ("ageUnknown", "future", "boundary", "pastBoundary")], ["unknown", "future", "current", "stale"])

    def test_longform_progress_and_counts_never_leak_into_ordinary_pipeline_metrics(self) -> None:
        result = self.run_browser_helpers('''
ui.app.state.autonomy.pipeline_telemetry = {asr_completed_items: 17};
const source = ui.telemetrySources()[0];
const overall = ui.deriveProgress();
const ordinary = ui.harvesterLifecycleMetrics().find(row => row.label === "ASR complete (ordinary queue)");
console.log(JSON.stringify({hasLongform: Object.hasOwn(source, "longform"),
  originalPreserved: ui.app.state.autonomy.longform === base, overall, ordinary}));
''')
        self.assertFalse(result["hasLongform"])
        self.assertTrue(result["originalPreserved"])
        self.assertIsNone(result["overall"]["percent"])
        self.assertEqual(result["ordinary"]["value"], "17")

    def test_longform_optional_counts_can_be_absent_without_inventing_zero(self) -> None:
        result = self.run_browser_helpers('''
const value = copy();
for (const key of ["cold_candidates", "queue_candidates", "expected_cold_backlog",
  "cold_candidates_not_discovered", "active_recordings"]) delete value.counts[key];
ui.app.state.autonomy.longform = value;
ui.renderLongform();
console.log(JSON.stringify({state: ui.normalizeLongform(value).state,
  labels: ui.elements["longform-counts"].children.map(row => row.children[0].textContent),
  progressHidden: ui.elements["longform-progress-section"].hidden}));
''')
        self.assertEqual(result["state"], "available")
        self.assertEqual(len(result["labels"]), 7)
        self.assertNotIn("Cold not yet discovered", result["labels"])
        self.assertNotIn("Active recordings", result["labels"])
        self.assertFalse(result["progressHidden"])

    def test_manual_stop_is_not_completion_but_known_partial_progress_is_preserved(self) -> None:
        result = self.run_browser_helpers('''
ui.app.state.autonomy.actual_state = "stopped";
const unknown = ui.deriveProgress();
ui.app.state.autonomy.progress = {completed: 2, total: 10};
const partial = ui.deriveProgress();
console.log(JSON.stringify({unknown, partial}));
''')
        self.assertIsNone(result["unknown"]["percent"])
        self.assertFalse(result["unknown"]["active"])
        self.assertIn("stopping does not establish", result["unknown"]["detail"])
        self.assertEqual(result["partial"]["percent"], 20)

    def test_assets_are_local_and_dom_rendering_is_text_only(self) -> None:
        combined = "\n".join((self.html, self.javascript, self.styles))
        self.assertNotRegex(combined, r"https?://")
        for unsafe in (
            "innerHTML",
            "outerHTML",
            "insertAdjacentHTML",
            "eval(",
            "new Function",
            "WebSocket",
            "EventSource",
        ):
            with self.subTest(unsafe=unsafe):
                self.assertNotIn(unsafe, combined)
        self.assertIn('<link rel="stylesheet" href="styles.css">', self.html)
        self.assertIn('<script src="app.js" defer></script>', self.html)

    def test_browser_contract_exposes_only_closed_autonomous_controls(self) -> None:
        self.assertIn('start: "autonomy.run"', self.javascript)
        self.assertIn('stop: "autonomy.request_stop"', self.javascript)
        self.assertIn("profile_id: binding.profile.profile_id", self.javascript)
        self.assertIn("expected_revision: app.state.revision", self.javascript)
        self.assertIn("preparation_token: prepared.preparation_token", self.javascript)
        self.assertIn("confirmation: null", self.javascript)
        self.assertNotIn("API.cancel", self.javascript)
        self.assertNotIn("<textarea", self.html)
        self.assertNotIn("<input", self.html)
        self.assertNotIn("<select", self.html)
        self.assertNotIn("<form", self.html)
        self.assertNotRegex(
            self.html,
            r'<(?:input|select)[^>]+name="(?:path|argv|command|environment|url|unit|pid)"',
        )
        self.assertNotIn('type="file"', self.html)
        self.assertNotIn('type="url"', self.html)
        self.assertIn("Commands, paths, and parameters remain bound server-side", self.html)
        buttons = re.findall(r'<button\b[^>]*\bid="([^"]+)"', self.html)
        self.assertEqual(buttons, ["start-pipeline", "stop-pipeline"])
        for removed in (
            "profile-list",
            "prepare-action",
            "confirmation-panel",
            "execute-confirmation",
            "cancel-confirmation",
            "refresh-state",
            "refresh-logs",
        ):
            with self.subTest(removed=removed):
                self.assertNotIn(f'id="{removed}"', self.html)

    def test_autonomous_monitors_cover_long_running_harvest(self) -> None:
        for monitor_id in (
            "overall-facts",
            "telemetry-age",
            "stage-list",
            "resource-list",
            "reservations-title",
            "collection-coverage",
            "pipeline-progress",
            "throughput-list",
            "storage-list",
            "pipeline-errors",
            "activity-list",
            "stdout-log",
            "stderr-log",
        ):
            with self.subTest(monitor_id=monitor_id):
                self.assertIn(f'id="{monitor_id}"', self.html)
        for lifecycle_label in (
            '"Discovered"',
            '"Queued"',
            '"Downloaded"',
            '"Preprocessed"',
            '"ASR complete"',
            '"Cold stored"',
            '"Parked"',
            '"Rate"',
            '"ETA"',
            '"Backpressure"',
        ):
            with self.subTest(lifecycle_label=lifecycle_label):
                self.assertIn(lifecycle_label, self.javascript)
        self.assertIn("collectionInventory", self.javascript)
        self.assertIn("MAX_MONITOR_METRICS", self.javascript)
        for status_field in (
            '"actual_state"',
            '"desired_state"',
            '"configured_schedule_count"',
            '"candidate_count"',
            '"ready_selected_count"',
            '"parked_requires_chunking_count"',
            '"last_cycle_new_acquisition_items"',
            '"acquisition_ready_bytes"',
        ):
            with self.subTest(status_field=status_field):
                self.assertIn(status_field, self.javascript)
        for hardcoded_campaign_value in ("3923", "3199", "724"):
            with self.subTest(hardcoded_campaign_value=hardcoded_campaign_value):
                self.assertNotIn(hardcoded_campaign_value, self.html + self.javascript)

    def test_lifecycle_counts_use_coherent_cumulative_controller_telemetry(self) -> None:
        for path in (
            '["pipeline_telemetry", "queued_items"]',
            '["pipeline_telemetry", "preprocessed_items"]',
            '["pipeline_telemetry", "asr_completed_items"]',
            '["monitor", "gpu_readiness", "completed_items"]',
        ):
            with self.subTest(path=path):
                self.assertIn(path, self.javascript)
        for stale_or_wrong_path in (
            '["monitor", "acquisition", "ready_items"]',
            '["monitor", "preprocess", "processed_items"]',
            '["progress", "asr", "completed"]',
        ):
            with self.subTest(stale_or_wrong_path=stale_or_wrong_path):
                self.assertNotIn(stale_or_wrong_path, self.javascript)
        self.assertIn("hasCoherentPipelineTelemetry", self.javascript)
        self.assertIn("coherentLifecycleMetric", self.javascript)
        self.assertIn(
            "selectCoherentLifecycleCount(",
            self.javascript,
        )
        self.assertIn(
            "value === null && !(hasCoherentPipelineTelemetry && coherentLifecycleMetric)",
            self.javascript,
        )

    def test_canonical_unavailable_count_cannot_leak_a_legacy_monitor_value(self) -> None:
        match = re.search(
            r"  function selectCoherentLifecycleCount\([^\n]+\) \{\n"
            r"    return [^\n]+;\n"
            r"  \}",
            self.javascript,
        )
        self.assertIsNotNone(match)
        script = (
            match.group(0)
            + "\nconsole.log(JSON.stringify(["
            + "selectCoherentLifecycleCount(true, null, 19),"
            + "selectCoherentLifecycleCount(false, null, 19),"
            + "selectCoherentLifecycleCount(true, 7, 19),"
            + "selectCoherentLifecycleCount(false, 7, 19)"
            + "]));"
        )
        completed = subprocess.run(
            ["node", "--input-type=commonjs", "--eval", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual("[null,19,7,7]", completed.stdout.strip())

    def test_stage_and_capacity_language_distinguishes_outcomes_from_utilization(self) -> None:
        combined = self.html + self.javascript
        for truthful_label in (
            "Latest stage outcomes",
            "Console launch reservations",
            "Admission gates only",
            "Running now",
            "Last result:",
            "operator_console_admission_reservations",
            "launch_admission_not_runtime_utilization",
            "conflicting_job_count",
            "Console job slots",
            "Reserved",
            "Open",
        ):
            with self.subTest(truthful_label=truthful_label):
                self.assertIn(truthful_label, combined)
        self.assertIn("currentControllerLaneIds", self.javascript)
        self.assertIn("autonomy?.lanes", self.javascript)
        self.assertIn('laneState === "running"', self.javascript)
        self.assertIn("activeLanes.map", self.javascript)
        self.assertNotIn("Current resource capacity", self.html)
        self.assertNotIn('state: active >= limit ? "Occupied"', self.javascript)

    def test_controller_telemetry_age_is_distinct_from_connection_freshness(self) -> None:
        self.assertIn('id="telemetry-age"', self.html)
        self.assertIn("function renderTelemetryAge()", self.javascript)
        self.assertIn('[["updated_at"]]', self.javascript)
        self.assertIn("lastResponseAt", self.javascript)
        self.assertIn("CONNECTION_STALE_MS", self.javascript)
        self.assertNotIn("lastSnapshotAt", self.javascript)
        self.assertNotIn('id="snapshot-age"', self.html)

    def test_controls_fail_closed_on_ambiguous_or_stale_bindings(self) -> None:
        self.assertIn('"not_started"', self.javascript)
        self.assertIn("hasActiveAction(ACTION_IDS.start)", self.javascript)
        for guard in (
            "profiles.length !== 1",
            "actions.length !== 1",
            "!app.stateFresh",
            "app.stateLoading",
            "app.mutationBusy",
            "unexpectedly requires typed confirmation",
            "The autonomous control binding changed during preparation",
        ):
            with self.subTest(guard=guard):
                self.assertIn(guard, self.javascript)

    def test_errors_hide_only_superseded_autonomous_runs(self) -> None:
        self.assertIn("function monitorErrorJobs()", self.javascript)
        self.assertIn("const latestRun = jobs.find", self.javascript)
        self.assertIn("job.action_id !== ACTION_IDS.start", self.javascript)
        self.assertIn("job.job_id === latestRun?.job_id", self.javascript)
        self.assertIn("for (const job of monitorErrorJobs())", self.javascript)
        self.assertIn("for (const job of app.state.jobs.filter(isPipelineJob))", self.javascript)

    def test_accessibility_and_bounded_polling_controls_are_present(self) -> None:
        for required in (
            'class="skip-link"',
            'role="alert"',
            'aria-live="polite"',
            'aria-busy',
            'prefers-reduced-motion: reduce',
            'forced-colors: active',
            'MAX_RESPONSE_CHARS',
            'MAX_LOG_CHARS',
            'AbortController',
        ):
            with self.subTest(required=required):
                self.assertIn(required, self.html + self.javascript + self.styles)
        ids = re.findall(r'\bid="([A-Za-z][A-Za-z0-9_-]*)"', self.html)
        self.assertEqual(len(ids), len(set(ids)), "HTML IDs must be unique")


if __name__ == "__main__":
    unittest.main()
