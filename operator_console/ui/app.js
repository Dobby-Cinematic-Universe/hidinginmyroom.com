(() => {
  "use strict";

  const API = Object.freeze({
    state: "api/state",
    prepare: "api/prepare",
    execute: "api/execute",
  });
  const ACTION_IDS = Object.freeze({
    start: "autonomy.run",
    stop: "autonomy.request_stop",
  });
  const LOG_STREAMS = Object.freeze(["stdout", "stderr"]);
  const MAX_RESPONSE_CHARS = 2_000_000;
  const MAX_LOG_CHARS = 200_000;
  const MAX_LOG_CHUNK_CHARS = 131_072;
  const MAX_LOG_RESPONSE_CHARS = 524_288;
  const MAX_METRIC_NODES = 512;
  const MAX_MONITOR_METRICS = 12;
  const MAX_ACTIVITY_ROWS = 8;
  const STATE_REQUEST_TIMEOUT_MS = 12_000;
  const MUTATION_REQUEST_TIMEOUT_MS = 20_000;
  const LOG_REQUEST_TIMEOUT_MS = 12_000;
  const CONNECTION_STALE_MS = 45_000;
  const LONGFORM_STALE_MS = 300_000;
  const ACTIVE_STATES = new Set([
    "launching", "starting", "running", "detached_running", "reconciling", "cancelling",
    "stopping", "stop_requested", "pending", "retrying",
  ]);
  const RUNNING_STATES = new Set([
    "launching", "starting", "running", "detached_running", "reconciling", "pending", "retrying",
  ]);
  const STOPPING_STATES = new Set(["cancelling", "stopping", "stop_requested", "stopped_pending"]);
  const SUCCESS_STATES = new Set([
    "available", "ready", "valid", "validated", "succeeded", "completed", "complete",
    "idle", "free", "open", "passed", "slots_available", "stopped", "not_started",
  ]);
  const DANGER_STATES = new Set([
    "blocked", "blocked_head", "failed", "indeterminate_after_restart",
    "cancelled_reconciliation_required", "invalid", "unavailable", "error", "degraded", "faulted",
  ]);
  const SAFE_OPAQUE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
  const SHA256 = /^[0-9a-f]{64}$/;

  const app = {
    state: null,
    stateFresh: false,
    stateLoading: false,
    lastResponseAt: null,
    mutationBusy: false,
    mutationKind: null,
    connectionError: null,
    controlError: null,
    stateTimer: null,
    clockTimer: null,
    observedJobId: null,
    logTimer: null,
    logPolling: false,
    logGeneration: 0,
    logAbort: null,
    logError: null,
    logs: {
      stdout: emptyLogState(),
      stderr: emptyLogState(),
    },
  };

  const elements = {};

  document.addEventListener("DOMContentLoaded", initialize);

  function initialize() {
    collectElements();
    bindEvents();
    scheduleClockTick();
    loadState({ initial: true }).catch((error) => failClosed(errorMessage(error)));
  }

  function collectElements() {
    const ids = [
      "main-content", "connection-badge", "aggregate-status", "error-summary", "error-list",
      "loading-panel", "invalid-panel", "invalid-detail", "console-workspace",
      "overall-state-badge", "pipeline-description", "control-note", "start-pipeline", "stop-pipeline",
      "telemetry-age", "overall-facts", "stage-list", "resource-list", "collection-total",
      "collection-coverage", "collection-empty", "progress-value",
      "pipeline-progress", "progress-detail", "throughput-list", "throughput-empty",
      "longform-age", "longform-detail", "longform-progress-section", "longform-progress",
      "longform-counts", "longform-lifecycle", "longform-error",
      "storage-list", "storage-empty", "monitor-error-count", "pipeline-errors", "errors-empty",
      "activity-count", "activity-history-note", "activity-list", "activity-empty", "output-job",
      "stdout-status", "stdout-truncation", "stdout-log", "stderr-status", "stderr-truncation",
      "stderr-log", "state-revision", "profile-set-digest",
    ];
    for (const id of ids) {
      const element = document.getElementById(id);
      if (!element) {
        throw new Error(`Required interface element is missing: ${id}`);
      }
      elements[id] = element;
    }
  }

  function bindEvents() {
    elements["start-pipeline"].addEventListener("click", () => operatePipeline("start"));
    elements["stop-pipeline"].addEventListener("click", () => operatePipeline("stop"));
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) {
        stopLogTimer();
        app.logAbort?.abort();
        return;
      }
      loadState({ background: true }).catch(() => {});
      pollObservedLogs();
    });
  }

  async function loadState({ initial = false, background = false } = {}) {
    if (app.stateLoading) {
      return;
    }
    app.stateLoading = true;
    if (initial) {
      showOnlyRootPanel("loading-panel");
      setConnection("Loading", "warning");
    }
    if (app.state) {
      renderControls();
    }

    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), STATE_REQUEST_TIMEOUT_MS);
    try {
      const payload = await requestJson(API.state, { method: "GET", signal: controller.signal });
      const normalized = validateState(payload);
      app.state = normalized;
      app.stateFresh = true;
      app.lastResponseAt = Date.now();
      app.connectionError = null;
      syncObservedJob();
      renderState();
      if (!app.controlError) {
        clearErrorSummary();
      }
      setConnection("Current", "success");
      if (!background) {
        announce("Current autonomous pipeline state loaded.");
      }
    } catch (error) {
      const message = error instanceof DOMException && error.name === "AbortError"
        ? "The pipeline state request timed out."
        : errorMessage(error);
      app.stateFresh = false;
      app.connectionError = message;
      setConnection("Unavailable", "danger");
      if (initial || app.state === null) {
        failClosed(message);
      } else {
        renderControls();
        renderErrors();
        announce("State refresh failed. Controls are disabled while the last validated snapshot remains visible.");
      }
    } finally {
      window.clearTimeout(timeout);
      app.stateLoading = false;
      if (app.state) {
        renderControls();
      }
      scheduleStateRefresh(app.stateFresh ? undefined : 5_000);
    }
  }

  function validateState(value) {
    if (!isPlainObject(value)) {
      throw new Error("State response must be a JSON object.");
    }
    if (typeof value.csrf_token !== "string" || value.csrf_token.length < 1 || value.csrf_token.length > 512) {
      throw new Error("State response has no bounded CSRF token.");
    }
    if (hasControlCharacter(value.csrf_token)) {
      throw new Error("State response CSRF token contains a control character.");
    }
    if (!Number.isSafeInteger(value.revision) || value.revision < 0) {
      throw new Error("State response has an invalid revision.");
    }
    if (!Array.isArray(value.profiles) || !Array.isArray(value.actions) || !Array.isArray(value.jobs)) {
      throw new Error("State response profiles, actions, and jobs must be arrays.");
    }
    if (value.blocked_capabilities !== undefined && !Array.isArray(value.blocked_capabilities)) {
      throw new Error("State response blocked capabilities must be an array.");
    }
    if (value.autonomy !== undefined && value.autonomy !== null && !isPlainObject(value.autonomy)) {
      throw new Error("State response autonomy monitor must be an object.");
    }
    const service = normalizeService(value.service);
    const profileSetSha256 = optionalText(value.profile_set_sha256, 64)
      || service.profile_set_sha256;
    if (!profileSetSha256 || !SHA256.test(profileSetSha256)) {
      throw new Error("State response has an invalid profile-set digest.");
    }
    const profiles = value.profiles.map((profile, index) => validateProfile(profile, index));
    const actions = value.actions.map((action, index) => validateAction(action, index));
    const jobs = value.jobs.map((job, index) => validateJob(job, index));
    const blocked = (value.blocked_capabilities || []).map(
      (item, index) => validateBlocked(item, index),
    );
    const jobHistoryCount = value.job_history_count === undefined
      ? jobs.length
      : safeCount(value.job_history_count);
    if (jobHistoryCount === null || jobHistoryCount < jobs.length) {
      throw new Error("State response has an invalid job-history count.");
    }
    if (value.job_history_truncated !== undefined && typeof value.job_history_truncated !== "boolean") {
      throw new Error("State response has an invalid job-history truncation flag.");
    }
    return {
      csrf_token: value.csrf_token,
      revision: value.revision,
      profile_set_sha256: profileSetSha256,
      service,
      profiles,
      actions,
      jobs,
      blocked_capabilities: blocked,
      job_history_count: jobHistoryCount,
      job_history_truncated: value.job_history_truncated === true,
      resources: normalizeCapacity(value.capacity),
      autonomy: value.autonomy ?? null,
      telemetry: isPlainObject(value.telemetry) ? value.telemetry : null,
      monitoring: isPlainObject(value.monitoring) ? value.monitoring : null,
    };
  }

  function normalizeService(value) {
    if (!isPlainObject(value)) {
      return {
        profile_set_sha256: null,
        implementation_version: null,
      };
    }
    return {
      profile_set_sha256: typeof value.profile_set_sha256 === "string" && SHA256.test(value.profile_set_sha256)
        ? value.profile_set_sha256
        : null,
      implementation_version: optionalText(value.implementation_version, 128),
    };
  }

  function validateProfile(value, index) {
    if (!isPlainObject(value)) {
      throw new Error(`Profile ${index + 1} must be an object.`);
    }
    const profileId = boundedText(value.profile_id, `Profile ${index + 1} ID`, 128);
    return {
      profile_id: profileId,
      label: boundedText(value.label, `Profile ${profileId} label`, 200),
      description: optionalText(value.description, 1_000),
      action_id: boundedText(value.action_id, `Profile ${profileId} action`, 128),
      stage: optionalText(value.stage, 200) || "Autonomous pipeline",
      effect: optionalText(value.effect, 64) || "unspecified",
      resource: optionalText(value.resource, 128) || "unspecified",
      supervisor: optionalText(value.supervisor, 64) || "direct",
      enabled: value.enabled === true,
      blocked_reason: optionalText(value.blocked_reason, 2_000),
      confirmation: optionalText(value.confirmation, 256),
      timeout_seconds: positiveIntegerOrNull(value.timeout_seconds),
      memory_max_bytes: positiveIntegerOrNull(value.memory_max_bytes),
      axes: readAxes(value),
    };
  }

  function validateAction(value, index) {
    if (!isPlainObject(value)) {
      throw new Error(`Action ${index + 1} must be an object.`);
    }
    const actionId = boundedText(value.action_id, `Action ${index + 1} ID`, 128);
    return {
      action_id: actionId,
      stage: optionalText(value.stage, 200) || "Autonomous pipeline",
      label: optionalText(value.label, 200) || actionId,
      description: optionalText(value.description, 1_000),
      effect: optionalText(value.effect, 64) || "unspecified",
      resource: optionalText(value.resource, 128) || "unspecified",
      supervisor: optionalText(value.supervisor, 64) || "direct",
      enabled: value.enabled === true,
      blocked_reason: optionalText(value.blocked_reason, 2_000),
      confirmation: optionalText(value.confirmation, 256),
      timeout_seconds: positiveIntegerOrNull(value.timeout_seconds),
      memory_max_bytes: positiveIntegerOrNull(value.memory_max_bytes),
    };
  }

  function validateBlocked(value, index) {
    if (!isPlainObject(value)) {
      throw new Error(`Blocked capability ${index + 1} must be an object.`);
    }
    return {
      stage: boundedText(value.stage, `Blocked capability ${index + 1} stage`, 300),
      reason: boundedText(value.reason, `Blocked capability ${index + 1} reason`, 3_000),
    };
  }

  function validateJob(value, index) {
    if (!isPlainObject(value)) {
      throw new Error(`Job ${index + 1} must be an object.`);
    }
    const jobId = boundedText(value.job_id ?? value.id, `Job ${index + 1} ID`, 256);
    if (!SAFE_OPAQUE_ID.test(jobId)) {
      throw new Error(`Job ${index + 1} has an unsafe opaque ID.`);
    }
    const status = optionalText(value.state ?? value.status, 128) || "not reported";
    const summary = isPlainObject(value.summary) ? value.summary : null;
    const axes = readAxes(value);
    axes.process ||= status;
    axes.durable ||= summaryAxis(summary, ["durable", "durable_state", "status"]);
    axes.gate ||= summaryAxis(summary, ["gate", "gate_state", "admission", "admission_state"]);
    return {
      job_id: jobId,
      profile_id: optionalText(value.profile_id, 128),
      action_id: optionalText(value.action_id, 128),
      label: optionalText(value.label, 300),
      stage: optionalText(value.stage, 200),
      effect: optionalText(value.effect, 64),
      resource: optionalText(value.resource, 128),
      supervisor: optionalText(value.supervisor, 64) || "direct",
      status,
      created_at: optionalText(value.created_at, 128),
      started_at: optionalText(value.started_at, 128),
      completed_at: optionalText(value.completed_at, 128),
      timeout_seconds: nonnegativeIntegerOrNull(value.timeout_seconds),
      deadline_exceeded: value.deadline_exceeded === true,
      exit_code: Number.isSafeInteger(value.returncode ?? value.exit_code)
        ? (value.returncode ?? value.exit_code)
        : null,
      error: optionalText(value.error, 3_000),
      checkpoint: checkpointText(value, summary),
      summary_sha256: summaryDigest(value, summary),
      axes,
      summary,
      telemetry: isPlainObject(value.telemetry) ? value.telemetry : null,
      logs: normalizeJobLogs(value.logs, jobId),
    };
  }

  function summaryAxis(summary, keys) {
    if (!summary) {
      return null;
    }
    for (const key of keys) {
      const candidate = summary[key];
      if (typeof candidate === "string") {
        return optionalText(candidate, 200);
      }
      if (isPlainObject(candidate)) {
        const label = optionalText(candidate.label ?? candidate.state ?? candidate.status, 200);
        if (label) {
          return label;
        }
      }
    }
    return null;
  }

  function checkpointText(value, summary) {
    const direct = optionalText(value.checkpoint, 500);
    if (direct) {
      return direct;
    }
    if (!summary) {
      return null;
    }
    const stopReason = optionalText(summary.stop_reason ?? summary.reason, 300);
    if (stopReason) {
      return stopReason;
    }
    const counts = isPlainObject(summary.counts) ? summary.counts : summary;
    const completed = safeCount(counts.completed_count ?? counts.completed ?? counts.done);
    const pending = safeCount(counts.pending_count ?? counts.pending ?? counts.remaining);
    if (completed !== null || pending !== null) {
      return [
        completed === null ? null : `${completed} completed`,
        pending === null ? null : `${pending} pending`,
      ].filter(Boolean).join(" · ");
    }
    if (summary.omitted === true) {
      return "Parsed summary omitted because it exceeded the public-state cap";
    }
    return null;
  }

  function summaryDigest(value, summary) {
    const candidates = [
      value.summary_sha256,
      summary?.canonical_sha256,
      summary?.summary_sha256,
      summary?.sha256,
      summary?.manifest_sha256,
      summary?.receipt_sha256,
    ];
    return candidates.find((candidate) => typeof candidate === "string" && SHA256.test(candidate)) || null;
  }

  function normalizeJobLogs(value, jobId) {
    const logs = {};
    for (const stream of LOG_STREAMS) {
      const item = isPlainObject(value?.[stream]) ? value[stream] : {};
      const available = item.available === true;
      logs[stream] = {
        available,
        byte_count: safeCount(item.byte_count),
        captured_byte_count: safeCount(item.captured_byte_count),
        sha256: typeof item.sha256 === "string" && SHA256.test(item.sha256) ? item.sha256 : null,
        truncated: item.truncated === true,
        base_url: available ? validatedLogBase(item.base_url, jobId, stream) : null,
      };
    }
    return logs;
  }

  function validatedLogBase(value, jobId, stream) {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value !== "string" || value.length > 2_048) {
      throw new Error(`Job ${jobId} has an invalid ${stream} log route.`);
    }
    let route;
    try {
      route = new URL(value, document.baseURI);
    } catch (_error) {
      throw new Error(`Job ${jobId} has an invalid ${stream} log route.`);
    }
    const expectedSuffix = `/api/jobs/${jobId}/logs/${stream}/`;
    if (
      route.origin !== window.location.origin
      || route.search !== ""
      || route.hash !== ""
      || !route.pathname.endsWith(expectedSuffix)
    ) {
      throw new Error(`Job ${jobId} has an unsafe ${stream} log route.`);
    }
    return route.href;
  }

  function normalizeCapacity(value) {
    if (!isPlainObject(value)) {
      throw new Error("State response capacity must be an object.");
    }
    const kind = optionalText(value.kind, 128);
    const semantics = optionalText(value.semantics, 128);
    if (kind !== null && kind !== "operator_console_admission_reservations") {
      throw new Error("State response capacity has an unsupported meaning.");
    }
    if (semantics !== null && semantics !== "launch_admission_not_runtime_utilization") {
      throw new Error("State response capacity has unsupported semantics.");
    }
    const globalLimit = safeCount(value.global_limit);
    const globalActive = safeCount(value.active_count);
    if (globalLimit === null || globalLimit < 1 || globalActive === null) {
      throw new Error("State response has invalid global capacity.");
    }
    if (value.global !== undefined) {
      if (!isPlainObject(value.global)) {
        throw new Error("State response global console capacity must be an object.");
      }
      const explicitLimit = safeCount(value.global.limit);
      const explicitActive = safeCount(value.global.active_job_count);
      const explicitAvailable = safeCount(value.global.available_slot_count);
      if (
        explicitLimit !== globalLimit
        || explicitActive !== globalActive
        || explicitAvailable !== Math.max(0, globalLimit - globalActive)
      ) {
        throw new Error("State response global console capacity is inconsistent.");
      }
    }
    const rows = [consoleJobCapacityRow(globalLimit, globalActive)];
    if (!isPlainObject(value.resources)) {
      throw new Error("State response capacity has no resource map.");
    }
    for (const [resourceId, item] of Object.entries(value.resources)) {
      if (!isPlainObject(item)) {
        throw new Error(`Capacity resource ${resourceId} must be an object.`);
      }
      const limit = safeCount(item.limit);
      const legacyActive = safeCount(item.active_count);
      const conflicting = item.conflicting_job_count === undefined
        ? legacyActive
        : safeCount(item.conflicting_job_count);
      if (limit === null || limit < 1 || legacyActive === null || conflicting === null) {
        throw new Error(`Capacity resource ${resourceId} is invalid.`);
      }
      if (legacyActive !== conflicting) {
        throw new Error(`Capacity resource ${resourceId} has inconsistent reservation counts.`);
      }
      if (item.blocked !== undefined && item.blocked !== (conflicting >= limit)) {
        throw new Error(`Capacity resource ${resourceId} has an inconsistent reservation state.`);
      }
      if (item.claim_ids !== undefined) {
        if (
          !Array.isArray(item.claim_ids)
          || item.claim_ids.length < 1
          || item.claim_ids.length > 16
          || item.claim_ids.some((claim) => (
            typeof claim !== "string"
            || !SAFE_OPAQUE_ID.test(claim)
          ))
        ) {
          throw new Error(`Capacity resource ${resourceId} has invalid reservation claims.`);
        }
      }
      rows.push(reservationRow(resourceId, titleCase(resourceId), limit, conflicting));
    }
    return rows;
  }

  function consoleJobCapacityRow(limit, active) {
    const available = Math.max(0, limit - active);
    return {
      id: "global",
      label: "Console job slots",
      state: active >= limit ? "At capacity" : "Slots available",
      detail: `${active} active console ${pluralize(active, "job")} · ${available} of ${limit} slots open`,
      available: active < limit,
    };
  }

  function reservationRow(id, label, limit, conflicting) {
    const blocked = conflicting >= limit;
    return {
      id,
      label,
      state: blocked ? "Reserved" : "Open",
      detail: conflicting === 0
        ? "No active console job conflicts with this launch class"
        : `${conflicting} active console ${pluralize(conflicting, "job")} ${conflicting === 1 ? "conflicts" : "conflict"} with this launch class`,
      available: !blocked,
    };
  }

  function pluralize(count, singular, plural = `${singular}s`) {
    return count === 1 ? singular : plural;
  }

  function renderState() {
    showOnlyRootPanel("console-workspace");
    elements["state-revision"].textContent = `State revision ${app.state.revision}`;
    elements["profile-set-digest"].textContent = `Profile set ${app.state.profile_set_sha256}`;
    const startBinding = resolveBinding("start");
    elements["pipeline-description"].textContent = startBinding.binding?.profile.description
      || startBinding.binding?.action.description
      || "The registered autonomous controller manages all stages and resource decisions.";
    renderControls();
    renderOverall();
    renderStagesAndResources();
    renderCollectionCoverage();
    renderProgressAndThroughput();
    renderLongform();
    renderStorage();
    renderErrors();
    renderActivity();
    renderOutput();
  }

  function resolveBinding(kind) {
    if (!app.state) {
      return { binding: null, issue: "No current pipeline state is loaded." };
    }
    const actionId = ACTION_IDS[kind];
    const profiles = app.state.profiles.filter((profile) => profile.action_id === actionId);
    if (profiles.length !== 1) {
      return {
        binding: null,
        issue: `Expected exactly one ${actionId} profile; received ${profiles.length}.`,
      };
    }
    const actions = app.state.actions.filter((action) => action.action_id === actionId);
    if (actions.length !== 1) {
      return {
        binding: null,
        issue: `Expected exactly one ${actionId} action; received ${actions.length}.`,
      };
    }
    const profile = profiles[0];
    const action = actions[0];
    if (!profile.enabled || !action.enabled) {
      return {
        binding: null,
        issue: profile.blocked_reason || action.blocked_reason || `${action.label} is disabled.`,
      };
    }
    for (const field of ["effect", "resource", "supervisor"]) {
      if (profile[field] !== action[field]) {
        return { binding: null, issue: `${actionId} has an inconsistent ${field} binding.` };
      }
    }
    if (profile.confirmation !== null || action.confirmation !== null) {
      return {
        binding: null,
        issue: `${actionId} unexpectedly requires typed confirmation; autonomous controls are disabled.`,
      };
    }
    return { binding: { profile, action }, issue: null };
  }

  function bindingIssues() {
    return [resolveBinding("start").issue, resolveBinding("stop").issue].filter(Boolean);
  }

  function renderControls() {
    if (!app.state) {
      elements["start-pipeline"].disabled = true;
      elements["stop-pipeline"].disabled = true;
      return;
    }
    const start = controlAvailability("start");
    const stop = controlAvailability("stop");
    elements["start-pipeline"].disabled = !start.available;
    elements["stop-pipeline"].disabled = !stop.available;
    elements["start-pipeline"].textContent = app.mutationKind === "start" ? "Starting…" : "Start";
    elements["stop-pipeline"].textContent = app.mutationKind === "stop" ? "Stopping…" : "Stop";

    let note;
    if (app.mutationBusy) {
      note = app.mutationKind === "start"
        ? "Submitting the closed autonomous start action…"
        : "Submitting the closed autonomous stop request…";
    } else if (!app.stateFresh) {
      note = "Controls are disabled until a current state snapshot is validated.";
    } else if (app.stateLoading) {
      note = "Checking current state before controls are made available.";
    } else if (start.issue && stop.issue && start.issue === stop.issue) {
      note = start.issue;
    } else if (pipelineIsStopping()) {
      note = "The controller is stopping the pipeline. Monitors will continue updating.";
    } else if (pipelineIsRunning() === true) {
      note = stop.available
        ? "The autonomous pipeline is running. Stop requests an orderly controller-managed shutdown."
        : stop.issue;
    } else {
      note = start.available
        ? "Start launches the single registered autonomous controller; all stage decisions remain server-bound."
        : start.issue;
    }
    elements["control-note"].textContent = note || "Controls are unavailable for the current state.";
  }

  function controlAvailability(kind) {
    const resolved = resolveBinding(kind);
    if (resolved.issue) {
      return { available: false, issue: resolved.issue };
    }
    if (!app.stateFresh) {
      return { available: false, issue: "The state snapshot is not current." };
    }
    if (app.stateLoading) {
      return { available: false, issue: "A state refresh is in progress." };
    }
    if (app.mutationBusy) {
      return { available: false, issue: "Another control request is in progress." };
    }
    if (kind === "start") {
      if (hasActiveAction(ACTION_IDS.stop)) {
        return { available: false, issue: "A stop request is still in progress." };
      }
      if (hasActiveAction(ACTION_IDS.start)) {
        return { available: false, issue: "The autonomous pipeline is already starting or running." };
      }
      const explicit = autonomyControlFlag("start");
      if (explicit === true) {
        return { available: true, issue: null };
      }
      if (explicit === false) {
        return { available: false, issue: "The autonomous controller is not ready to start." };
      }
      const running = pipelineIsRunning();
      if (running === true) {
        return { available: false, issue: "The autonomous pipeline is already running." };
      }
      if (running === null && app.state.autonomy !== null) {
        return { available: false, issue: "The autonomous controller did not report whether Start is safe." };
      }
      return { available: true, issue: null };
    }

    if (hasActiveAction(ACTION_IDS.stop) || pipelineIsStopping()) {
      return { available: false, issue: "An orderly stop request is already in progress." };
    }
    const explicit = autonomyControlFlag("stop");
    if (explicit === true) {
      return { available: true, issue: null };
    }
    if (explicit === false) {
      return { available: false, issue: "The autonomous controller is not currently stoppable." };
    }
    return pipelineIsRunning() === true
      ? { available: true, issue: null }
      : { available: false, issue: "The autonomous pipeline is not running." };
  }

  function autonomyControlFlag(kind) {
    const autonomy = app.state?.autonomy;
    if (!autonomy) {
      return null;
    }
    const paths = kind === "start"
      ? [["can_start"], ["start_enabled"], ["controls", "can_start"], ["controls", "start_enabled"]]
      : [["can_stop"], ["stop_enabled"], ["controls", "can_stop"], ["controls", "stop_enabled"]];
    for (const path of paths) {
      const value = readPath(autonomy, path);
      if (typeof value === "boolean") {
        return value;
      }
    }
    return null;
  }

  function pipelineIsRunning() {
    const autonomy = app.state?.autonomy;
    if (autonomy && typeof autonomy.running === "boolean") {
      return autonomy.running;
    }
    const status = autonomyStatus({ allowJobFallback: false });
    if (RUNNING_STATES.has(normalizeStateText(status))) {
      return true;
    }
    if (activePipelineJobs().some((job) => job.action_id === ACTION_IDS.start)) {
      return true;
    }
    const desired = normalizeStateText(firstText(autonomy, [["desired_state"]], 128));
    if (desired === "running") {
      return true;
    }
    if (SUCCESS_STATES.has(normalizeStateText(status)) || STOPPING_STATES.has(normalizeStateText(status))) {
      return false;
    }
    const latestRun = app.state?.jobs.find((job) => job.action_id === ACTION_IDS.start) ?? null;
    const summaryStatus = latestRun?.summary
      ? firstText(latestRun.summary, [["pipeline_status"], ["controller_status"], ["status"], ["state"]], 128)
      : null;
    if (RUNNING_STATES.has(normalizeStateText(summaryStatus))) {
      return true;
    }
    if (summaryStatus && (SUCCESS_STATES.has(normalizeStateText(summaryStatus)) || DANGER_STATES.has(normalizeStateText(summaryStatus)))) {
      return false;
    }
    return autonomy === null ? false : null;
  }

  function pipelineIsStopping() {
    const status = autonomyStatus({ allowJobFallback: false });
    const actual = normalizeStateText(status);
    const desired = normalizeStateText(firstText(app.state?.autonomy, [["desired_state"]], 128));
    return STOPPING_STATES.has(actual)
      || (desired === "stopped" && RUNNING_STATES.has(actual))
      || hasActiveAction(ACTION_IDS.stop);
  }

  function hasActiveAction(actionId) {
    return activePipelineJobs().some((job) => job.action_id === actionId);
  }

  function activePipelineJobs() {
    return (app.state?.jobs || []).filter(
      (job) => Object.values(ACTION_IDS).includes(job.action_id)
        && ACTIVE_STATES.has(normalizeStateText(job.status)),
    );
  }

  async function operatePipeline(kind) {
    const availability = controlAvailability(kind);
    const resolved = resolveBinding(kind);
    if (!availability.available || !resolved.binding) {
      return;
    }
    app.controlError = null;
    clearErrorSummary();
    setMutationBusy(true, kind);
    announce(kind === "start" ? "Starting the autonomous pipeline…" : "Requesting an orderly stop…");
    try {
      const job = await prepareAndExecute(resolved.binding);
      app.controlError = null;
      renderState();
      announce(kind === "start"
        ? `Autonomous start accepted as ${job.job_id}.`
        : `Autonomous stop request accepted as ${job.job_id}.`);
      scheduleStateRefresh(1_000);
    } catch (error) {
      await handleMutationError(error, kind === "start"
        ? "The autonomous pipeline could not be started."
        : "The autonomous stop request was not accepted.");
    } finally {
      setMutationBusy(false, null);
    }
  }

  async function prepareAndExecute(binding) {
    const preparedResponse = await postJson(API.prepare, {
      profile_id: binding.profile.profile_id,
      expected_revision: app.state.revision,
    });
    if (
      !isPlainObject(preparedResponse)
      || !isPlainObject(preparedResponse.state)
      || !isPlainObject(preparedResponse.prepared)
    ) {
      throw new Error("Prepare response must contain current state and a prepared action.");
    }
    const preparedState = validateState(preparedResponse.state);
    adoptState(preparedState);
    const current = resolveBinding(binding.action.action_id === ACTION_IDS.start ? "start" : "stop");
    if (!current.binding || current.binding.profile.profile_id !== binding.profile.profile_id) {
      throw new Error("The autonomous control binding changed during preparation.");
    }
    const prepared = validatePreparation(preparedResponse.prepared, current.binding, preparedState);
    if (prepared.expires_at !== null) {
      const expiry = Date.parse(prepared.expires_at);
      if (!Number.isFinite(expiry) || Date.now() >= expiry) {
        throw new Error("The prepared autonomous action expired before execution.");
      }
    }

    const executeResponse = await postJson(API.execute, {
      preparation_token: prepared.preparation_token,
      expected_revision: app.state.revision,
      confirmation: null,
    });
    if (
      !isPlainObject(executeResponse)
      || !isPlainObject(executeResponse.state)
      || !isPlainObject(executeResponse.job)
    ) {
      throw new Error("Execute response must contain current state and the accepted job.");
    }
    const executedState = validateState(executeResponse.state);
    const returnedJobId = boundedText(executeResponse.job.job_id, "Accepted job ID", 256);
    const job = executedState.jobs.find((item) => item.job_id === returnedJobId);
    if (!job || job.action_id !== binding.action.action_id || job.profile_id !== binding.profile.profile_id) {
      throw new Error("The accepted job does not match the prepared autonomous action.");
    }
    adoptState(executedState);
    return job;
  }

  function validatePreparation(value, binding, state) {
    const preview = isPlainObject(value.preview) ? value.preview : value;
    const preparationToken = boundedText(
      value.preparation_token ?? preview.preparation_token,
      "Preparation token",
      512,
    );
    if (hasControlCharacter(preparationToken)) {
      throw new Error("Preparation token contains a control character.");
    }
    if (boundedText(preview.profile_id, "Prepared profile ID", 128) !== binding.profile.profile_id) {
      throw new Error("Prepared profile does not match the autonomous control binding.");
    }
    if (boundedText(preview.action_id, "Prepared action ID", 128) !== binding.action.action_id) {
      throw new Error("Prepared action does not match the autonomous control binding.");
    }
    const profileSet = boundedText(preview.profile_set_sha256, "Prepared profile-set digest", 64);
    if (!SHA256.test(profileSet) || profileSet !== state.profile_set_sha256) {
      throw new Error("Prepared action does not match the current profile set.");
    }
    for (const field of ["effect", "resource", "supervisor"]) {
      const preparedValue = boundedText(preview[field], `Prepared ${field}`, 128);
      if (preparedValue !== binding.profile[field] || preparedValue !== binding.action[field]) {
        throw new Error(`Prepared ${field} does not match the autonomous control binding.`);
      }
    }
    if (preview.confirmation !== null && preview.confirmation !== undefined && preview.confirmation !== "") {
      throw new Error("Prepared autonomous action unexpectedly requires typed confirmation.");
    }
    return {
      preparation_token: preparationToken,
      expires_at: optionalText(value.expires_at, 128),
    };
  }

  function adoptState(state) {
    app.state = state;
    app.stateFresh = true;
    app.lastResponseAt = Date.now();
    app.connectionError = null;
    setConnection("Current", "success");
    syncObservedJob();
    renderState();
  }

  async function handleMutationError(error, fallback) {
    let message = errorMessage(error, fallback);
    if (error instanceof DOMException && error.name === "AbortError") {
      message = `${fallback} The server response timed out; current state will be reloaded before controls are enabled.`;
    } else if (error instanceof HttpError && error.status === 409) {
      const code = isPlainObject(error.payload) ? optionalText(error.payload.error, 128) : null;
      const detail = isPlainObject(error.payload) ? optionalText(error.payload.message, 500) : null;
      message = code === "stale_revision"
        ? "Pipeline state changed during the request. Current state was reloaded; review the monitors before trying again."
        : detail || fallback;
    }
    app.stateFresh = false;
    await loadState({ background: true }).catch(() => {});
    app.controlError = message;
    renderControls();
    renderErrors();
    showErrors([message]);
    announce(message);
  }

  function setMutationBusy(busy, kind) {
    app.mutationBusy = busy;
    app.mutationKind = kind;
    document.body.setAttribute("aria-busy", busy ? "true" : "false");
    renderControls();
  }

  function renderOverall() {
    const overall = overallStatus();
    setBadge(elements["overall-state-badge"], titleCase(overall.label), overall.tone);
    renderOverallFacts();
    renderTelemetryAge();
  }

  function overallStatus() {
    const issues = bindingIssues();
    if (issues.length > 0) {
      return { label: "Controls blocked", tone: "danger" };
    }
    const status = autonomyStatus();
    if (status) {
      return { label: status, tone: toneForState(status) };
    }
    return { label: "Idle", tone: "success" };
  }

  function autonomyStatus({ allowJobFallback = true } = {}) {
    const autonomy = app.state?.autonomy;
    if (autonomy) {
      const direct = firstText(autonomy, [
        ["actual_state"], ["lifecycle"], ["status"], ["state"], ["overall_status"], ["overall_state"],
        ["controller", "status"], ["controller", "state"],
        ["process", "status"], ["process", "state"],
      ], 128);
      if (direct) {
        return direct;
      }
      if (typeof autonomy.running === "boolean") {
        return autonomy.running ? "running" : "idle";
      }
    }
    if (!allowJobFallback) {
      return null;
    }
    return monitoredJob()?.status || null;
  }

  function renderOverallFacts() {
    if (!app.state) {
      return;
    }
    const job = monitoredJob();
    const autonomy = app.state.autonomy;
    const startedAt = firstText(autonomy, [
      ["started_at"], ["run", "started_at"], ["controller", "started_at"],
    ], 128) || job?.started_at || job?.created_at || null;
    const completedAt = firstText(autonomy, [
      ["stopped_at"], ["completed_at"], ["run", "completed_at"],
    ], 128) || job?.completed_at || null;
    const runId = firstText(autonomy, [
      ["run_id"], ["current_run_id"], ["run", "id"], ["current_job_id"], ["job_id"],
    ], 256) || job?.job_id || "No run recorded";
    const elapsed = startedAt ? formatElapsed(startedAt, completedAt) : "Not available";
    const desiredState = firstText(autonomy, [["desired_state"]], 128) || "Not reported";
    const activeLanes = [...currentControllerLaneIds(autonomy)];
    const currentStage = activeLanes.length > 0
      ? activeLanes.map((stage) => titleCase(stage)).join(" + ")
      : firstText(autonomy, [["current_stage"]], 200) || "Between stages";
    const cycle = firstFiniteNumber(autonomy || {}, ["cycle"]);
    const failures = firstFiniteNumber(autonomy || {}, ["consecutive_failures"]);
    const updatedAt = firstText(autonomy, [["updated_at"]], 128);
    renderFacts(elements["overall-facts"], [
      ["Actual state", titleCase(overallStatus().label), false],
      ["Desired state", titleCase(desiredState), false],
      ["Current stage", titleCase(currentStage), false],
      ["Cycle", cycle === null ? "Not reported" : cycle.toLocaleString("en-US"), false],
      ["Run", runId, true],
      ["Started", formatTime(startedAt) || "Not reported", false],
      [completedAt ? "Duration" : "Elapsed", elapsed, false],
      ["Consecutive failures", failures === null ? "Not reported" : failures.toLocaleString("en-US"), false],
      ["Last update", formatTime(updatedAt) || "Not reported", false],
    ]);
  }

  function renderTelemetryAge() {
    const updatedAt = firstText(app.state?.autonomy, [["updated_at"]], 128);
    const updatedMilliseconds = updatedAt === null ? NaN : Date.parse(updatedAt);
    if (!Number.isFinite(updatedMilliseconds)) {
      elements["telemetry-age"].textContent = "Telemetry age unknown";
      elements["telemetry-age"].dataset.tone = "warning";
      return;
    }
    const ageMilliseconds = Math.max(0, Date.now() - updatedMilliseconds);
    elements["telemetry-age"].textContent = ageMilliseconds < 2_000
      ? "Telemetry just updated"
      : `Telemetry ${formatDurationMilliseconds(ageMilliseconds)} old`;
    elements["telemetry-age"].dataset.tone = ageMilliseconds > CONNECTION_STALE_MS
      ? "warning"
      : "success";
  }

  function renderStagesAndResources() {
    const stages = normalizedStages();
    const stageList = elements["stage-list"];
    stageList.replaceChildren();
    for (const stage of stages) {
      const card = document.createElement("section");
      card.className = "stage-card";
      const heading = document.createElement("h3");
      const outcome = document.createElement("span");
      heading.textContent = stage.label;
      outcome.textContent = `Last result: ${titleCase(stage.status || "not reported")}`;
      outcome.className = "stage-outcome";
      outcome.dataset.tone = toneForState(stage.status);
      card.append(heading);
      if (stage.running) {
        const live = document.createElement("span");
        live.textContent = "Running now";
        live.className = "stage-live";
        live.dataset.tone = "warning";
        card.append(live);
      }
      card.append(outcome);
      stageList.append(card);
    }

    const resources = elements["resource-list"];
    resources.replaceChildren();
    for (const resource of app.state.resources) {
      const row = document.createElement("div");
      row.className = "resource-card";
      const term = document.createElement("dt");
      const status = document.createElement("dd");
      const detail = document.createElement("dd");
      term.textContent = resource.label;
      status.textContent = resource.state;
      status.className = "resource-state";
      status.dataset.tone = toneForState(resource.state);
      detail.textContent = resource.detail;
      detail.className = "resource-detail";
      row.append(term, status, detail);
      resources.append(row);
    }
  }

  function normalizedStages() {
    const autonomy = app.state.autonomy;
    const raw = autonomy?.stages ?? autonomy?.stage_status ?? autonomy?.stage_states;
    const running = currentControllerLaneIds(autonomy);
    const rows = [];
    if (Array.isArray(raw)) {
      for (const [index, item] of raw.slice(0, 16).entries()) {
        if (typeof item === "string") {
          const label = `Stage ${index + 1}`;
          const id = normalizeStateText(label);
          rows.push({ id, label, status: optionalText(item, 200), running: running.has(id) });
        } else if (isPlainObject(item)) {
          const label = optionalText(item.label ?? item.name ?? item.stage ?? item.id, 200)
            || `Stage ${index + 1}`;
          const id = normalizeStateText(optionalText(item.id ?? item.stage ?? item.name, 200) || label);
          rows.push({
            id,
            label,
            status: optionalText(item.status ?? item.state ?? item.process_state, 200) || "not reported",
            running: running.has(id),
          });
        }
      }
    } else if (isPlainObject(raw)) {
      for (const [key, item] of Object.entries(raw).slice(0, 16)) {
        const id = normalizeStateText(key);
        rows.push({
          id,
          label: titleCase(key),
          status: typeof item === "string"
            ? optionalText(item, 200)
            : isPlainObject(item)
              ? optionalText(item.status ?? item.state ?? item.label, 200)
              : "not reported",
          running: running.has(id),
        });
      }
    }
    if (rows.length > 0) {
      return rows;
    }
    const job = monitoredJob();
    const startProfile = resolveBinding("start").binding?.profile;
    const axes = hasAxes(readAxes(autonomy || {}))
      ? readAxes(autonomy)
      : hasAxes(job?.axes || {})
        ? job.axes
        : startProfile?.axes || {};
    return [
      { id: "process", label: "Process", status: axes.process || autonomyStatus() || "not reported", running: false },
      { id: "durable_state", label: "Durable state", status: axes.durable || "not reported", running: false },
      { id: "admission_gate", label: "Admission gate", status: axes.gate || "not reported", running: false },
    ];
  }

  function currentControllerLaneIds(autonomy) {
    const running = new Set();
    const lanes = autonomy?.lanes;
    if (isPlainObject(lanes)) {
      for (const [laneId, lane] of Object.entries(lanes).slice(0, 16)) {
        if (!isPlainObject(lane)) {
          continue;
        }
        const laneState = normalizeStateText(optionalText(lane.state, 128) || "");
        if (lane.active === true || lane.active === 1 || laneState === "running") {
          const normalized = normalizeStateText(laneId);
          if (normalized) {
            running.add(normalized);
          }
        }
      }
    }
    const currentStage = firstText(autonomy, [["current_stage"]], 200);
    if (!currentStage) {
      return running;
    }
    for (const stage of currentStage.split("+")) {
      const normalized = normalizeStateText(stage);
      if (normalized && !["between_stages", "none", "idle", "concurrent"].includes(normalized)) {
        running.add(normalized);
      }
    }
    return running;
  }

  function renderCollectionCoverage() {
    const coverage = collectionCoverageMetrics();
    renderMetrics(elements["collection-coverage"], coverage.metrics);
    elements["collection-empty"].hidden = coverage.metrics.length > 0;
    elements["collection-total"].textContent = coverage.total === null
      ? "Not reported"
      : `${coverage.total.toLocaleString("en-US")} collections`;
  }

  function collectionCoverageMetrics() {
    const autonomy = app.state.autonomy;
    const explicit = [
      autonomy?.campaign,
      autonomy?.collection_coverage,
      autonomy?.collections_summary,
      autonomy?.discovery,
      autonomy?.coverage,
    ].filter(isPlainObject);
    const sources = explicit.length > 0 ? explicit : telemetrySources();
    const metrics = [];
    const usedPaths = new Set();
    const definitions = [
      ["Known collections", [
        "collection_count", "collections_total", "total_collections", "known_collections",
        ...(explicit.length > 0 ? ["total"] : []),
      ]],
      ["Inventory candidates", ["candidate_count", "inventory_candidate_count", "total_candidates"]],
      ["Ready selected", ["ready_selected_count", "selected_ready_count"]],
      ["Parked", ["parked_requires_chunking_count", "parked_count"]],
      ["Selected bytes", ["estimated_selected_bytes", "selected_byte_count"]],
      ["Inventory duration", ["total_duration_ms", "inventory_duration_ms"]],
      ["Ready duration", ["ready_selected_duration_ms", "selected_duration_ms"]],
      ["Configured schedules", ["configured_schedule_count", "schedule_count"]],
      ["Collections discovered", ["collections_discovered", "discovered_collections", "collection_discovered_count"]],
      ["Collections complete", ["collections_complete", "completed_collections", "collection_complete_count"]],
      ["Collections active", ["collections_active", "active_collections", "collection_active_count"]],
      ["Collections pending", ["collections_pending", "pending_collections", "collection_pending_count"]],
      ["Collections with errors", ["collections_failed", "failed_collections", "collection_error_count"]],
      ["Videos discovered", ["videos_discovered", "discovered_video_count", "discovered_items", "discovery_total"]],
    ];
    let total = null;
    for (const [label, aliases] of definitions) {
      const found = findNumericByKeys(sources, aliases, usedPaths);
      if (!found) {
        continue;
      }
      usedPaths.add(found.path.join("\u0000"));
      metrics.push({
        label,
        value: /bytes/i.test(label)
          ? formatBytes(found.value)
          : /duration/i.test(label)
            ? formatDurationMilliseconds(found.value)
          : formatNumber(found.value, Number.isInteger(found.value) ? 0 : 2),
      });
      if (label === "Known collections") {
        total = found.value;
      }
    }

    const inventory = collectionInventory(autonomy?.collections);
    if (inventory) {
      if (total === null) {
        total = inventory.total;
        metrics.unshift({ label: "Known collections", value: inventory.total.toLocaleString("en-US") });
      }
      const existing = new Set(metrics.map((metric) => metric.label));
      for (const [state, count] of inventory.states) {
        const label = `Collections · ${titleCase(state)}`;
        if (!existing.has(label) && metrics.length < MAX_MONITOR_METRICS) {
          metrics.push({ label, value: count.toLocaleString("en-US") });
          existing.add(label);
        }
      }
      if (inventory.discoveredVideos !== null && !existing.has("Videos discovered")) {
        metrics.push({
          label: "Videos discovered",
          value: inventory.discoveredVideos.toLocaleString("en-US"),
        });
      }
    }

    if (metrics.length === 0) {
      const generic = collectMetrics(
        sources,
        /(collection|coverage|discover|known|complete|active|pending|video|items?)/i,
      );
      mergeUniqueMetrics(metrics, generic, MAX_MONITOR_METRICS);
    }
    return { metrics: metrics.slice(0, MAX_MONITOR_METRICS), total };
  }

  function collectionInventory(raw) {
    let rows;
    if (Array.isArray(raw)) {
      rows = raw.filter(isPlainObject);
    } else if (isPlainObject(raw) && Object.values(raw).some(isPlainObject)) {
      rows = Object.values(raw).filter(isPlainObject);
    } else {
      return null;
    }
    const states = new Map();
    let discoveredVideos = 0;
    let hasVideoCount = false;
    for (const row of rows) {
      const state = normalizeStateText(
        optionalText(row.status ?? row.state ?? row.discovery_state, 128) || "not reported",
      );
      states.set(state, (states.get(state) || 0) + 1);
      const count = firstFiniteNumber(row, [
        "videos_discovered", "discovered_video_count", "discovered_count", "item_count", "total_count",
      ]);
      if (count !== null && count >= 0) {
        discoveredVideos += count;
        hasVideoCount = true;
      }
    }
    return {
      total: rows.length,
      states: [...states.entries()].sort((left, right) => right[1] - left[1]),
      discoveredVideos: hasVideoCount ? discoveredVideos : null,
    };
  }

  function renderProgressAndThroughput() {
    const progress = deriveProgress();
    const bar = elements["pipeline-progress"];
    if (progress.percent === null) {
      bar.removeAttribute("value");
      bar.textContent = progress.active ? "In progress" : "Completion not reported";
      elements["progress-value"].textContent = progress.active ? "In progress" : "Not reported";
    } else {
      const bounded = Math.max(0, Math.min(100, progress.percent));
      bar.value = bounded;
      bar.textContent = `${Math.round(bounded)}%`;
      elements["progress-value"].textContent = `${formatNumber(bounded, 1)}%`;
    }
    elements["progress-detail"].textContent = progress.detail;

    const metrics = harvesterLifecycleMetrics();
    renderMetrics(elements["throughput-list"], metrics);
    elements["throughput-empty"].hidden = metrics.length > 0;
  }

  function normalizeLongform(value) {
    const unavailable = (state, diagnostic) => ({
      state, diagnostic, counts: null, completion_percent: null,
      updated_at: null, lifecycle: null, last_error: null,
    });
    if (value === null || value === undefined) {
      return unavailable("not_reported", "missing_projection");
    }
    if (!isPlainObject(value) || value.schema_version !== 1
        || !["available", "not_registered", "unavailable"].includes(value.state)
        || value.basis !== "cached_companion_status_completed_recording_jobs") {
      return unavailable("unavailable", "invalid_projection");
    }
    const result = unavailable(value.state, optionalText(value.diagnostic, 128));
    if (value.updated_at !== null && value.updated_at !== undefined) {
      if (typeof value.updated_at !== "string" || value.updated_at.length > 128
          || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value.updated_at)
          || !Number.isFinite(Date.parse(value.updated_at))) {
        return unavailable("unavailable", "invalid_timestamp");
      }
      result.updated_at = value.updated_at;
    }
    const lifecycle = optionalText(value.lifecycle, 128);
    result.lifecycle = lifecycle && !hasControlCharacter(lifecycle) ? lifecycle : null;
    if (isPlainObject(value.last_error)) {
      const type = optionalText(value.last_error.type, 128);
      const message = optionalText(value.last_error.message, 800);
      if (type && message && !hasControlCharacter(type) && !hasControlCharacter(message)) {
        result.last_error = { type, message };
      }
    }
    if (value.state !== "available") {
      return result;
    }
    if (!isPlainObject(value.counts)) {
      return unavailable("unavailable", "invalid_counts");
    }
    const counts = {};
    for (const key of [
      "completed_recordings", "discovered_recordings", "remaining_discovered_recordings",
      "unprepared_recordings", "preprocessed_recordings", "prepared_recordings", "incomplete_recordings",
    ]) {
      const count = safeCount(value.counts[key]);
      if (count === null) {
        return unavailable("unavailable", "invalid_counts");
      }
      counts[key] = count;
    }
    for (const key of ["cold_candidates", "queue_candidates", "expected_cold_backlog", "cold_candidates_not_discovered", "active_recordings"]) {
      if (value.counts[key] !== null && value.counts[key] !== undefined && safeCount(value.counts[key]) === null) {
        return unavailable("unavailable", "invalid_counts");
      }
      counts[key] = safeCount(value.counts[key]);
    }
    const completed = counts.completed_recordings;
    const discovered = counts.discovered_recordings;
    const percent = discovered === 0 ? null : completed / discovered * 100;
    const stageTotal = completed + counts.unprepared_recordings + counts.preprocessed_recordings
      + counts.prepared_recordings + counts.incomplete_recordings;
    const hasCandidateSplit = counts.cold_candidates !== null && counts.queue_candidates !== null;
    const hasColdBacklog = counts.expected_cold_backlog !== null && counts.cold_candidates !== null
      && counts.cold_candidates_not_discovered !== null;
    if (completed > discovered || stageTotal !== discovered
        || counts.remaining_discovered_recordings !== discovered - completed
        || (hasCandidateSplit && counts.cold_candidates + counts.queue_candidates !== discovered)
        || (hasColdBacklog && counts.expected_cold_backlog - counts.cold_candidates !== counts.cold_candidates_not_discovered)
        || (counts.active_recordings !== null && counts.active_recordings > 1)
        || (percent === null && value.completion_percent !== null)
        || (percent !== null && (typeof value.completion_percent !== "number"
          || !Number.isFinite(value.completion_percent) || value.completion_percent < 0
          || value.completion_percent > 100 || Math.abs(value.completion_percent - percent) > 0.1))) {
      return unavailable("unavailable", "inconsistent_progress");
    }
    return { ...result, counts, completion_percent: percent };
  }

  function longformSnapshot(value, now = Date.now()) {
    const snapshot = normalizeLongform(value);
    const updated = snapshot.updated_at === null ? NaN : Date.parse(snapshot.updated_at);
    const age = now - updated;
    return {
      ...snapshot,
      age_ms: Number.isFinite(age) && age >= -60_000 ? Math.max(0, age) : null,
      freshness: !Number.isFinite(age) ? "unknown"
        : age < -60_000 ? "future" : age > LONGFORM_STALE_MS ? "stale" : "current",
    };
  }

  function renderLongform() {
    const snapshot = longformSnapshot(app.state?.autonomy?.longform);
    const available = snapshot.state === "available";
    const current = available && snapshot.freshness === "current";
    const age = elements["longform-age"];
    age.dataset.tone = current ? "success" : "warning";
    if (snapshot.state === "not_registered") {
      age.textContent = "Not registered";
    } else if (!available) {
      age.textContent = snapshot.state === "not_reported" ? "Not reported" : "Unavailable";
    } else if (snapshot.age_ms === null) {
      age.textContent = snapshot.freshness === "future" ? "Companion clock mismatch" : "Companion age unknown";
    } else {
      age.textContent = `${snapshot.freshness === "stale" ? "Stale · " : ""}Companion ${formatDurationMilliseconds(snapshot.age_ms)} old`;
    }
    const metrics = available ? [
      ["Long-form ASR complete", "completed_recordings"],
      ["Discovered recordings", "discovered_recordings"],
      ["Remaining discovered", "remaining_discovered_recordings"],
      ["Cold not yet discovered", "cold_candidates_not_discovered"],
      ["Unprepared", "unprepared_recordings"],
      ["Preprocessed", "preprocessed_recordings"],
      ["Prepared", "prepared_recordings"],
      ["Incomplete", "incomplete_recordings"],
      ["Active recordings", "active_recordings"],
    ].filter(([, key]) => snapshot.counts[key] !== null).map(([label, key]) => ({
      label, value: formatNumber(snapshot.counts[key], 0),
    })) : [];
    renderMetrics(elements["longform-counts"], metrics);
    elements["longform-counts"].hidden = !available;
    const bar = elements["longform-progress"];
    elements["longform-progress-section"].hidden = !current || snapshot.completion_percent === null;
    bar.value = current && snapshot.completion_percent !== null ? snapshot.completion_percent : 0;
    bar.textContent = `${formatNumber(bar.value, 1)}% of discovered recordings`;
    let detail;
    if (snapshot.state === "not_registered") {
      detail = "No long-form companion is registered for this controller.";
    } else if (snapshot.state === "not_reported") {
      detail = "Long-form companion statistics are not reported by this console snapshot.";
    } else if (!available) {
      detail = "Long-form companion statistics are unavailable or invalid. This does not change Start or Stop availability.";
    } else {
      detail = snapshot.counts.discovered_recordings === 0
        ? "No long-form recordings have been discovered in this cached companion snapshot."
        : `${formatNumber(snapshot.counts.completed_recordings, 0)} of ${formatNumber(snapshot.counts.discovered_recordings, 0)} discovered recordings completed (${formatNumber(snapshot.completion_percent, 1)}%).`;
      if (!current) {
        detail = `Last cached snapshot only; ${snapshot.freshness === "stale" ? "companion telemetry is over 5 minutes old" : "companion freshness cannot be confirmed"}. ${detail}`;
      }
    }
    elements["longform-detail"].textContent = detail;
    elements["longform-lifecycle"].hidden = !available || !snapshot.lifecycle;
    elements["longform-lifecycle"].textContent = snapshot.lifecycle
      ? `${current ? "Companion lifecycle" : "Last reported companion lifecycle"}: ${titleCase(snapshot.lifecycle)}` : "";
    elements["longform-error"].hidden = snapshot.last_error === null;
    elements["longform-error"].textContent = snapshot.last_error
      ? `Companion last error: ${snapshot.last_error.type}: ${snapshot.last_error.message}` : "";
  }

  function harvesterLifecycleMetrics() {
    const sources = telemetrySources();
    const metrics = [];
    const usedPaths = new Set();
    const autonomy = app.state.autonomy;
    const hasCoherentPipelineTelemetry = isPlainObject(autonomy?.pipeline_telemetry);
    const acquiredCompleted = firstNumberAtPaths(autonomy, [
      ["progress", "acquisition", "completed"],
      ["stages", "acquisition", "completed"],
      ["monitor", "acquisition", "completed"],
    ]);
    const acquiredPending = firstNumberAtPaths(autonomy, [
      ["progress", "acquisition", "pending"],
      ["stages", "acquisition", "pending"],
      ["monitor", "acquisition", "pending"],
    ]);
    const lifecycle = [
      ["Discovered", ["discovered", "discovered_count", "discovered_items", "videos_discovered", "discovery_total"], [
        ["campaign", "candidate_count"], ["campaign", "totals", "candidate_count"],
      ]],
      ["Queued", ["queued", "queued_count", "queue_count", "ready_item_count", "pending_download_count"], [
        ["pipeline_telemetry", "queued_items"],
      ]],
      ["Downloaded", ["downloaded", "downloaded_count", "acquired_count", "acquisition_count"], [
        ["progress", "acquisition", "completed"], ["stages", "acquisition", "completed"],
        ["monitor", "acquisition", "completed"],
      ]],
      ["Preprocessed", ["preprocessed", "preprocessed_count", "preprocess_completed_count"], [
        ["pipeline_telemetry", "preprocessed_items"],
      ]],
      ["ASR complete", ["asr_complete", "asr_completed", "asr_completed_count", "transcribed_count"], [
        ["pipeline_telemetry", "asr_completed_items"],
      ]],
      ["Cold stored", ["cold_stored", "cold_stored_count", "cold_retained_items", "retained_count", "archived_count"], [
        ["storage", "cold_retained_items"], ["progress", "cold_retention", "retained_items"],
        ["stages", "cold_retention", "retained_items"],
      ]],
      ["Parked", ["parked", "parked_count", "parked_requires_chunking_count", "deferred_count"], [
        ["campaign", "parked_requires_chunking_count"],
        ["campaign", "totals", "parked_requires_chunking_count"],
      ]],
    ];
    for (const [label, aliases, paths] of lifecycle) {
      let value = firstNumberAtPaths(autonomy, paths);
      if (label === "Discovered" && value === null && acquiredCompleted !== null && acquiredPending !== null) {
        value = acquiredCompleted + acquiredPending;
      }
      const coherentLifecycleMetric = ["Queued", "Preprocessed", "ASR complete"].includes(label);
      const legacyAsrItems = label === "ASR complete"
        ? firstNumberAtPaths(autonomy, [
          ["progress", "gpu_readiness", "completed_items"],
          ["stages", "gpu_readiness", "completed_items"],
          ["monitor", "gpu_readiness", "completed_items"],
        ])
        : null;
      value = selectCoherentLifecycleCount(
        hasCoherentPipelineTelemetry && coherentLifecycleMetric,
        value,
        legacyAsrItems,
      );
      if (value === null && !(hasCoherentPipelineTelemetry && coherentLifecycleMetric)) {
        const found = findNumericByKeys(sources, aliases, usedPaths);
        if (found) {
          usedPaths.add(found.path.join("\u0000"));
          value = found.value;
        }
      }
      metrics.push({
        label: label === "ASR complete" ? "ASR complete (ordinary queue)" : label,
        value: value === null
          ? "Not reported"
          : formatNumber(value, Number.isInteger(value) ? 0 : 2),
      });
    }

    const lastCycleItems = firstNumberAtPaths(autonomy, [["throughput", "last_cycle_new_acquisition_items"]]);
    const lastCycleBytes = firstNumberAtPaths(autonomy, [["throughput", "last_cycle_new_acquisition_bytes"]]);
    if (lastCycleItems !== null) {
      metrics.push({ label: "Last cycle · New items", value: lastCycleItems.toLocaleString("en-US") });
    }
    if (lastCycleBytes !== null) {
      metrics.push({ label: "Last cycle · New bytes", value: formatBytes(lastCycleBytes) });
    }
    const operational = collectMetrics(
      sources,
      /(_rate$|_rate_|rate$|per_second$|per_minute$|per_hour$|eta$|eta_seconds$|eta_at$|estimated_completion$|estimated_completion_seconds$|estimated_completion_at$)/i,
    );
    const rate = operational.find((metric) => /(rate|per second|per minute|per hour)/i.test(metric.label));
    metrics.push(rate || { label: "Rate", value: "Not reported" });

    const eta = findScalarByKeys(sources, ["eta", "estimated_completion", "estimated_completion_at"]);
    const numericEta = operational.find((metric) => /ETA|estimated completion/i.test(metric.label));
    if (eta !== null) {
      const value = typeof eta === "string" ? (formatTime(eta) || eta) : String(eta);
      metrics.push({ label: "ETA", value });
    } else {
      metrics.push(numericEta || { label: "ETA", value: "Not reported" });
    }
    let backpressure = findScalarByKeys(sources, [
      "backpressure", "backpressure_state", "backpressure_status", "backpressure_reason",
    ]);
    if (backpressure === null) {
      const stopReason = firstText(autonomy, [
        ["stages", "acquisition", "stop_reason"],
        ["monitor", "acquisition", "stop_reason"],
      ], 300);
      if (stopReason && /(backpressure|ready_high|global_ready|capacity|free_space)/i.test(stopReason)) {
        backpressure = stopReason;
      }
    }
    const backpressureValue = backpressure === null
      ? "Not reported"
      : typeof backpressure === "boolean"
        ? (backpressure ? "Active" : "Clear")
        : titleCase(String(backpressure));
    metrics.push({ label: "Backpressure", value: backpressureValue });
    return metrics.slice(0, MAX_MONITOR_METRICS);
  }

  function selectCoherentLifecycleCount(hasCoherentTelemetry, canonicalValue, legacyValue) {
    return hasCoherentTelemetry ? canonicalValue : (canonicalValue ?? legacyValue);
  }

  function deriveProgress() {
    const sources = telemetrySources();
    const numeric = flattenNumeric(sources);
    const percent = numeric.find((metric) => /(percent|percentage|progress_pct|progress_percent)$/i.test(metric.key));
    if (percent && percent.value >= 0 && percent.value <= 100) {
      return {
        percent: percent.value,
        active: pipelineIsRunning() === true,
        detail: progressDetail() || `${humanizeMetricPath(percent.path)} reported by the controller.`,
      };
    }
    const pair = findProgressPair(sources);
    if (pair) {
      return {
        percent: pair.total === 0 ? 0 : (pair.current / pair.total) * 100,
        active: pipelineIsRunning() === true,
        detail: `${pair.current.toLocaleString("en-US")} of ${pair.total.toLocaleString("en-US")} ${pair.label}.`,
      };
    }
    const job = monitoredJob();
    const state = normalizeStateText(autonomyStatus() || job?.status);
    if (["succeeded", "completed", "complete", "validated", "passed"].includes(state)) {
      return { percent: 100, active: false, detail: progressDetail() || "The latest run reached a terminal state." };
    }
    if (state === "stopped") {
      return { percent: null, active: false, detail: "The pipeline is stopped; stopping does not establish campaign completion." };
    }
    if (pipelineIsRunning() === true || pipelineIsStopping()) {
      return { percent: null, active: true, detail: progressDetail() || "Work is active; no aggregate percentage is reported." };
    }
    return { percent: 0, active: false, detail: progressDetail() || "The pipeline is idle." };
  }

  function findProgressPair(sources) {
    const stack = sources.filter(isPlainObject).reverse().map((value) => ({ value, depth: 0 }));
    let visited = 0;
    while (stack.length > 0 && visited < MAX_METRIC_NODES) {
      const current = stack.pop();
      visited += 1;
      if (!current || current.depth > 5) {
        continue;
      }
      const value = current.value;
      const total = firstFiniteNumber(value, ["total", "total_count", "job_count", "item_count", "selected_count"]);
      const completed = firstFiniteNumber(value, [
        "completed", "completed_count", "processed", "processed_count", "done", "current",
        "adapter_invocation_count",
      ]);
      if (total !== null && total >= 0 && completed !== null && completed >= 0 && completed <= total) {
        return { current: completed, total, label: "items completed" };
      }
      const pending = firstFiniteNumber(value, ["pending", "pending_count", "remaining", "remaining_count"]);
      if (completed !== null && completed >= 0 && pending !== null && pending >= 0) {
        return { current: completed, total: completed + pending, label: "items completed" };
      }
      for (const child of Object.values(value)) {
        if (isPlainObject(child)) {
          stack.push({ value: child, depth: current.depth + 1 });
        }
      }
    }
    return null;
  }

  function progressDetail() {
    const autonomy = app.state.autonomy;
    return firstText(autonomy, [
      ["progress", "detail"], ["progress", "message"], ["checkpoint"], ["status_detail"],
      ["current_stage"],
    ], 500) || monitoredJob()?.checkpoint || null;
  }

  function renderStorage() {
    const autonomy = app.state.autonomy;
    const metrics = [];
    const storageFields = [
      ["Acquisition ready", [["storage", "acquisition_ready_bytes"]], "bytes"],
      ["Selected inventory", [["campaign", "estimated_selected_bytes"]], "bytes"],
      ["New last cycle", [["throughput", "last_cycle_new_acquisition_bytes"]], "bytes"],
      ["Cold retained", [["storage", "cold_retained_items"]], "count"],
      ["Hot deletions", [["storage", "hot_deletions"]], "count"],
    ];
    for (const [label, paths, kind] of storageFields) {
      const value = firstNumberAtPaths(autonomy, paths);
      if (value !== null) {
        metrics.push({
          label,
          value: kind === "bytes" ? formatBytes(value) : formatNumber(value, 0),
        });
      }
    }
    const generic = collectMetrics(
      telemetrySources(),
      /(bytes?|byte_count|storage|disk|space|size|capacity|reserved|reservation)/i,
    ).filter((metric) => !/(acquisition ready|estimated selected|last cycle new acquisition|cold retained|hot deletions)/i.test(metric.label));
    mergeUniqueMetrics(metrics, generic, MAX_MONITOR_METRICS);
    renderMetrics(elements["storage-list"], metrics);
    elements["storage-empty"].hidden = metrics.length > 0;
  }

  function telemetrySources() {
    const job = monitoredJob();
    const autonomy = app.state?.autonomy;
    // Discovered-only companion progress must never become aggregate progress.
    const controllerTelemetry = isPlainObject(autonomy)
      ? Object.fromEntries(Object.entries(autonomy).filter(([key]) => key !== "longform"))
      : null;
    return [
      controllerTelemetry,
      app.state?.telemetry,
      app.state?.monitoring,
      job?.telemetry,
      job?.summary,
    ].filter(isPlainObject);
  }

  function collectMetrics(sources, matcher) {
    const excluded = /(sha|digest|version|ordinal|schema|uid|pid|exit_code|returncode|timeout|maximum|minimum)/i;
    const result = [];
    const seen = new Set();
    for (const metric of flattenNumeric(sources)) {
      const pathText = metric.path.join("_");
      if (!matcher.test(pathText) || excluded.test(pathText)) {
        continue;
      }
      const label = humanizeMetricPath(metric.path);
      if (seen.has(label)) {
        continue;
      }
      seen.add(label);
      result.push({ label, value: formatMetricValue(metric.value, pathText) });
      if (result.length >= MAX_MONITOR_METRICS) {
        break;
      }
    }
    return result;
  }

  function findNumericByKeys(sources, aliases, excludedPaths = new Set()) {
    const available = flattenNumeric(sources);
    for (const alias of aliases.map(normalizeMetricKey)) {
      for (const metric of available) {
        const pathKey = metric.path.join("\u0000");
        if (!excludedPaths.has(pathKey) && normalizeMetricKey(metric.key) === alias) {
          return metric;
        }
      }
    }
    return null;
  }

  function findScalarByKeys(sources, aliases) {
    const wanted = new Set(aliases.map(normalizeMetricKey));
    const stack = sources.filter(isPlainObject).reverse().map((value) => ({ value, depth: 0 }));
    let visited = 0;
    while (stack.length > 0 && visited < MAX_METRIC_NODES) {
      const current = stack.pop();
      visited += 1;
      if (!current || current.depth > 6) {
        continue;
      }
      for (const [key, value] of Object.entries(current.value)) {
        if (
          wanted.has(normalizeMetricKey(key))
          && (typeof value === "string" || typeof value === "boolean" || typeof value === "number")
        ) {
          return value;
        }
        if (isPlainObject(value)) {
          stack.push({ value, depth: current.depth + 1 });
        }
      }
    }
    return null;
  }

  function normalizeMetricKey(value) {
    return String(value).trim().toLowerCase().replaceAll("-", "_").replaceAll(" ", "_");
  }

  function mergeUniqueMetrics(target, additions, maximum) {
    const labels = new Set(target.map((metric) => metric.label));
    for (const metric of additions) {
      if (!labels.has(metric.label)) {
        target.push(metric);
        labels.add(metric.label);
      }
      if (target.length >= maximum) {
        break;
      }
    }
  }

  function flattenNumeric(sources) {
    const roots = Array.isArray(sources) ? sources : [sources];
    const stack = roots.filter(isPlainObject).reverse().map((value) => ({ value, path: [], depth: 0 }));
    const result = [];
    let visited = 0;
    while (stack.length > 0 && visited < MAX_METRIC_NODES) {
      const current = stack.pop();
      visited += 1;
      if (!current || current.depth > 6) {
        continue;
      }
      for (const [key, value] of Object.entries(current.value).slice(0, 100).reverse()) {
        const path = [...current.path, key];
        if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
          result.push({ key, path, value });
        } else if (Array.isArray(value)) {
          if (/(items?|cycles?|errors?|events?|activity|results?)/i.test(key)) {
            result.push({ key: `${key}_count`, path: [...path, "count"], value: value.length });
          }
          for (const child of value.slice(0, 24).reverse()) {
            if (isPlainObject(child)) {
              stack.push({ value: child, path, depth: current.depth + 1 });
            }
          }
        } else if (isPlainObject(value)) {
          stack.push({ value, path, depth: current.depth + 1 });
        }
      }
    }
    return result;
  }

  function humanizeMetricPath(path) {
    const useful = path
      .filter((part) => !["autonomy", "summary", "telemetry", "monitoring", "progress"].includes(part))
      .slice(-2);
    return useful.map(titleCase).join(" · ") || "Metric";
  }

  function formatMetricValue(value, path) {
    if (/bytes?|byte_count/i.test(path)) {
      return formatBytes(value);
    }
    if (/duration_ms|elapsed_ms|milliseconds/i.test(path)) {
      return formatDurationMilliseconds(value);
    }
    if (/duration_seconds|elapsed_seconds|_seconds/i.test(path)) {
      return formatDurationMilliseconds(value * 1_000);
    }
    if (/eta_seconds|estimated_seconds/i.test(path)) {
      return formatDurationMilliseconds(value * 1_000);
    }
    if (/percent|percentage|_pct/i.test(path)) {
      return `${formatNumber(value, 1)}%`;
    }
    if (/per_second/i.test(path)) {
      return `${formatNumber(value, 2)}/s`;
    }
    if (/per_minute/i.test(path)) {
      return `${formatNumber(value, 2)}/min`;
    }
    if (/per_hour/i.test(path)) {
      return `${formatNumber(value, 2)}/h`;
    }
    return formatNumber(value, Number.isInteger(value) ? 0 : 2);
  }

  function renderMetrics(container, metrics) {
    container.replaceChildren();
    for (const metric of metrics) {
      const row = document.createElement("div");
      const term = document.createElement("dt");
      const value = document.createElement("dd");
      term.textContent = metric.label;
      value.textContent = String(metric.value);
      row.append(term, value);
      container.append(row);
    }
  }

  function renderErrors() {
    const errors = collectMonitorErrors();
    const list = elements["pipeline-errors"];
    list.replaceChildren();
    for (const error of errors) {
      const item = document.createElement("li");
      const source = document.createElement("strong");
      const message = document.createElement("span");
      source.textContent = error.source;
      message.textContent = error.message;
      item.append(source, message);
      list.append(item);
    }
    elements["monitor-error-count"].textContent = String(errors.length);
    elements["monitor-error-count"].dataset.tone = errors.length > 0 ? "danger" : "success";
    elements["errors-empty"].hidden = errors.length > 0;
  }

  function collectMonitorErrors() {
    const rows = [];
    if (app.connectionError) {
      rows.push({ source: "Connection", message: app.connectionError });
    }
    if (app.controlError) {
      rows.push({ source: "Control request", message: app.controlError });
    }
    for (const issue of bindingIssues()) {
      rows.push({ source: "Autonomous controls", message: issue });
    }
    for (const error of extractStructuredErrors(app.state?.autonomy, "Controller")) {
      rows.push(error);
    }
    for (const job of monitorErrorJobs()) {
      if (job.error) {
        rows.push({ source: job.label || job.job_id, message: job.error });
      } else if (job.deadline_exceeded) {
        rows.push({ source: job.label || job.job_id, message: "The runtime deadline was exceeded." });
      } else if (DANGER_STATES.has(normalizeStateText(job.status))) {
        rows.push({ source: job.label || job.job_id, message: `Run state: ${titleCase(job.status)}.` });
      }
      for (const error of extractStructuredErrors(job.summary, job.label || job.job_id)) {
        rows.push(error);
      }
    }
    if (app.logError) {
      rows.push({ source: "Log monitor", message: app.logError });
    }
    const unique = [];
    const seen = new Set();
    for (const row of rows) {
      const key = row.message;
      if (!seen.has(key)) {
        seen.add(key);
        unique.push(row);
      }
      if (unique.length >= 12) {
        break;
      }
    }
    return unique;
  }

  function monitorErrorJobs() {
    const jobs = (app.state?.jobs || []).filter(isPipelineJob);
    const latestRun = jobs.find((job) => job.action_id === ACTION_IDS.start);
    return jobs
      .filter((job) => (
        job.action_id !== ACTION_IDS.start
        || job.job_id === latestRun?.job_id
      ))
      .slice(0, 8);
  }

  function extractStructuredErrors(root, source) {
    if (!isPlainObject(root)) {
      return [];
    }
    const result = [];
    const stack = [{ value: root, path: [], depth: 0 }];
    let visited = 0;
    while (stack.length > 0 && visited < 160 && result.length < 6) {
      const current = stack.pop();
      visited += 1;
      if (!current || current.depth > 4) {
        continue;
      }
      for (const [key, value] of Object.entries(current.value)) {
        const path = [...current.path, key];
        const errorKey = /(error|errors|failure|failed_job|last_error)/i.test(key);
        if (errorKey && typeof value === "string" && optionalText(value, 1_000)) {
          result.push({ source: `${source} · ${humanizeMetricPath(path)}`, message: value });
        } else if (errorKey && Array.isArray(value)) {
          for (const item of value.slice(0, 6)) {
            const message = structuredMessage(item);
            if (message) {
              result.push({ source: `${source} · ${titleCase(key)}`, message });
            }
          }
        } else if (isPlainObject(value)) {
          if (errorKey) {
            const message = structuredMessage(value);
            if (message) {
              result.push({ source: `${source} · ${titleCase(key)}`, message });
            }
          }
          stack.push({ value, path, depth: current.depth + 1 });
        }
      }
    }
    return result;
  }

  function structuredMessage(value) {
    if (typeof value === "string") {
      return optionalText(value, 1_000);
    }
    if (!isPlainObject(value)) {
      return null;
    }
    const code = optionalText(value.code ?? value.type, 128);
    const detail = optionalText(value.message ?? value.reason ?? value.detail ?? value.error, 800);
    return [code, detail].filter(Boolean).join(": ") || null;
  }

  function renderActivity() {
    const rows = recentActivity();
    const list = elements["activity-list"];
    list.replaceChildren();
    for (const row of rows) {
      const item = document.createElement("li");
      const heading = document.createElement("div");
      heading.className = "activity-heading";
      const title = document.createElement("strong");
      const badge = document.createElement("span");
      const detail = document.createElement("p");
      title.textContent = row.title;
      badge.className = "status-badge";
      setBadge(badge, titleCase(row.status || "recorded"), toneForState(row.status));
      detail.textContent = [row.time ? formatTime(row.time) : null, row.detail].filter(Boolean).join(" · ") || "No additional detail";
      heading.append(title, badge);
      item.append(heading, detail);
      if (row.id) {
        const id = document.createElement("code");
        id.textContent = row.id;
        item.append(id);
      }
      list.append(item);
    }
    elements["activity-count"].textContent = String(rows.length);
    elements["activity-empty"].hidden = rows.length > 0;
    elements["activity-history-note"].hidden = !app.state.job_history_truncated;
    elements["activity-history-note"].textContent = app.state.job_history_truncated
      ? `Showing recent activity from ${app.state.job_history_count} recorded control jobs.`
      : "";
  }

  function recentActivity() {
    const result = [];
    const raw = app.state.autonomy?.recent_activity
      ?? app.state.autonomy?.activity
      ?? app.state.autonomy?.events;
    if (Array.isArray(raw)) {
      for (const [index, item] of raw.slice(0, MAX_ACTIVITY_ROWS).entries()) {
        if (typeof item === "string" && optionalText(item, 800)) {
          result.push({ title: "Controller activity", status: "recorded", detail: item, time: null, id: null });
        } else if (isPlainObject(item)) {
          result.push({
            title: optionalText(item.title ?? item.label ?? item.action ?? item.stage ?? item.event_type, 300) || `Activity ${index + 1}`,
            status: optionalText(item.status ?? item.state ?? item.level, 128) || "recorded",
            detail: optionalText(item.detail ?? item.message ?? item.reason, 800)
              || (Number.isSafeInteger(item.sequence) ? `Event ${item.sequence}` : null),
            time: optionalText(item.at ?? item.timestamp ?? item.created_at ?? item.completed_at, 128),
            id: optionalText(item.id ?? item.job_id ?? item.run_id, 256),
          });
        }
      }
    }
    for (const job of app.state.jobs.filter(isPipelineJob)) {
      if (result.length >= MAX_ACTIVITY_ROWS) {
        break;
      }
      result.push({
        title: job.label || actionLabel(job.action_id) || job.job_id,
        status: job.status,
        detail: job.checkpoint || job.error || activityTiming(job),
        time: job.completed_at || job.started_at || job.created_at,
        id: job.job_id,
      });
    }
    return result.slice(0, MAX_ACTIVITY_ROWS);
  }

  function activityTiming(job) {
    if (!job.started_at) {
      return "Waiting to start";
    }
    return job.completed_at
      ? `Completed in ${formatElapsed(job.started_at, job.completed_at)}`
      : `Running for ${formatElapsed(job.started_at, null)}`;
  }

  function actionLabel(actionId) {
    return app.state.actions.find((action) => action.action_id === actionId)?.label || null;
  }

  function isPipelineJob(job) {
    return Object.values(ACTION_IDS).includes(job.action_id);
  }

  function syncObservedJob() {
    const next = chooseObservedJob();
    if (next?.job_id !== app.observedJobId) {
      app.observedJobId = next?.job_id || null;
      resetLogs();
    }
  }

  function chooseObservedJob() {
    if (!app.state) {
      return null;
    }
    const requestedId = firstText(app.state.autonomy, [
      ["current_job_id"], ["job_id"], ["run", "job_id"], ["current_run", "job_id"],
    ], 256);
    if (requestedId) {
      const requested = app.state.jobs.find((job) => job.job_id === requestedId && isPipelineJob(job));
      if (requested) {
        return requested;
      }
    }
    return activePipelineJobs().find((job) => job.action_id === ACTION_IDS.start)
      || activePipelineJobs()[0]
      || app.state.jobs.find((job) => job.action_id === ACTION_IDS.start)
      || app.state.jobs.find(isPipelineJob)
      || null;
  }

  function monitoredJob() {
    return app.state?.jobs.find((job) => job.job_id === app.observedJobId) || chooseObservedJob();
  }

  function renderOutput() {
    const job = monitoredJob();
    elements["output-job"].textContent = job ? compactJobId(job.job_id) : "No run";
    renderLogStream("stdout");
    renderLogStream("stderr");
    if (job) {
      scheduleLogPoll(0);
    }
  }

  async function pollObservedLogs() {
    const job = monitoredJob();
    if (!job || app.logPolling || document.hidden) {
      return;
    }
    app.logPolling = true;
    app.logError = null;
    stopLogTimer();
    const generation = app.logGeneration;
    app.logAbort?.abort();
    app.logAbort = new AbortController();
    const timeout = window.setTimeout(() => app.logAbort?.abort(), LOG_REQUEST_TIMEOUT_MS);
    try {
      const available = LOG_STREAMS.filter((stream) => job.logs[stream].available);
      await Promise.all(available.map(
        (stream) => pollLogStream(job, stream, generation, app.logAbort.signal),
      ));
    } finally {
      window.clearTimeout(timeout);
      app.logPolling = false;
      renderErrors();
      if (generation === app.logGeneration) {
        scheduleLogPoll();
      }
    }
  }

  async function pollLogStream(job, stream, generation, signal) {
    const current = app.logs[stream];
    if (!job.logs[stream].available || current.eof) {
      renderLogStream(stream);
      return;
    }
    const route = job.logs[stream].base_url
      ? `${job.logs[stream].base_url}${current.offset}`
      : `api/jobs/${encodeURIComponent(job.job_id)}/logs/${stream}/${current.offset}`;
    try {
      const response = await fetch(route, {
        method: "GET",
        credentials: "same-origin",
        cache: "no-store",
        redirect: "error",
        headers: { Accept: "application/json" },
        signal,
      });
      if (!response.ok) {
        throw new HttpError(response.status, `Log request returned HTTP ${response.status}.`, null);
      }
      const chunk = await readLogResponse(response, stream, current.offset);
      const captured = job.logs[stream].captured_byte_count;
      if (captured !== null && chunk.nextOffset > captured) {
        throw new Error("Log response advanced beyond the advertised captured byte count.");
      }
      if (generation !== app.logGeneration || monitoredJob()?.job_id !== job.job_id) {
        return;
      }
      appendLog(stream, chunk);
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        if (!document.hidden && generation === app.logGeneration) {
          current.error = "Log request timed out.";
          app.logError = current.error;
          renderLogStream(stream);
        }
        return;
      }
      current.error = errorMessage(error);
      app.logError = current.error;
      renderLogStream(stream);
    }
  }

  async function readLogResponse(response, expectedStream, requestedOffset) {
    const contentType = response.headers.get("Content-Type") || "";
    if (!contentType.toLowerCase().startsWith("application/json")) {
      throw new Error("Log response is not JSON.");
    }
    const payload = await readJsonResponse(response, MAX_LOG_RESPONSE_CHARS);
    if (!isPlainObject(payload)) {
      throw new Error("Log response must be an object.");
    }
    if (payload.stream !== expectedStream) {
      throw new Error("Log response stream does not match the request.");
    }
    if (!Number.isSafeInteger(payload.offset) || payload.offset !== requestedOffset) {
      throw new Error("Log response offset does not match the request.");
    }
    if (!Number.isSafeInteger(payload.next_offset) || payload.next_offset < payload.offset) {
      throw new Error("Log response has an invalid next offset.");
    }
    if (typeof payload.eof !== "boolean" || typeof payload.truncated !== "boolean") {
      throw new Error("Log response has invalid completion metadata.");
    }
    if (payload.next_offset === payload.offset && !payload.eof) {
      throw new Error("Log response made no progress before end of stream.");
    }
    if (typeof payload.text !== "string" || payload.text.length > MAX_LOG_CHUNK_CHARS) {
      throw new Error("Log response text is missing or exceeds its client bound.");
    }
    return {
      text: payload.text,
      nextOffset: payload.next_offset,
      eof: payload.eof,
      truncated: payload.truncated,
    };
  }

  function appendLog(stream, chunk) {
    const target = app.logs[stream];
    if (chunk.nextOffset < target.offset) {
      target.error = "Server log offset moved backwards; polling stopped for this stream.";
      app.logError = target.error;
      renderLogStream(stream);
      return;
    }
    target.text += chunk.text;
    target.offset = chunk.nextOffset;
    target.eof = chunk.eof;
    target.serverTruncated = target.serverTruncated || chunk.truncated;
    target.error = null;
    if (target.text.length > MAX_LOG_CHARS) {
      target.text = target.text.slice(-MAX_LOG_CHARS);
      target.clientTruncated = true;
    }
    renderLogStream(stream);
  }

  function renderLogStream(stream) {
    const state = app.logs[stream];
    const metadata = monitoredJob()?.logs?.[stream] ?? null;
    const log = elements[`${stream}-log`];
    const wasNearEnd = log.scrollHeight - log.scrollTop - log.clientHeight < 48;
    log.textContent = state.text;
    if (wasNearEnd) {
      log.scrollTop = log.scrollHeight;
    }
    const status = state.error
      ? `Polling error · offset ${state.offset}`
      : metadata && !metadata.available
        ? "Available after the control job exits"
        : state.eof
          ? `End of stream · offset ${state.offset}`
          : metadata?.available
            ? `Reading · offset ${state.offset} of ${metadata.captured_byte_count ?? "unknown"}`
            : "Not available";
    elements[`${stream}-status`].textContent = status;
    const notices = [];
    if (state.serverTruncated || metadata?.truncated) {
      notices.push("The server reports earlier log bytes were truncated.");
    }
    if (state.clientTruncated) {
      notices.push(`The browser retains only the most recent ${MAX_LOG_CHARS.toLocaleString("en-US")} characters.`);
    }
    if (state.error) {
      notices.push(state.error);
    }
    const notice = elements[`${stream}-truncation`];
    notice.textContent = notices.join(" ");
    notice.hidden = notices.length === 0;
  }

  function resetLogs() {
    app.logGeneration += 1;
    app.logAbort?.abort();
    app.logAbort = null;
    app.logError = null;
    app.logs.stdout = emptyLogState();
    app.logs.stderr = emptyLogState();
    stopLogTimer();
    renderLogStream("stdout");
    renderLogStream("stderr");
  }

  function emptyLogState() {
    return {
      text: "",
      offset: 0,
      eof: false,
      serverTruncated: false,
      clientTruncated: false,
      error: null,
    };
  }

  function scheduleStateRefresh(delay) {
    if (app.stateTimer !== null) {
      window.clearTimeout(app.stateTimer);
    }
    const active = pipelineIsRunning() === true || activePipelineJobs().length > 0;
    const milliseconds = delay ?? (active ? 3_000 : 12_000);
    app.stateTimer = window.setTimeout(() => {
      app.stateTimer = null;
      if (!document.hidden && !app.mutationBusy) {
        loadState({ background: true }).catch(() => {});
      } else {
        scheduleStateRefresh();
      }
    }, milliseconds);
  }

  function scheduleLogPoll(delay = 1_500) {
    stopLogTimer();
    const job = monitoredJob();
    const unread = job && LOG_STREAMS.some(
      (stream) => job.logs[stream].available && !app.logs[stream].eof,
    );
    if (!unread) {
      return;
    }
    app.logTimer = window.setTimeout(() => {
      app.logTimer = null;
      pollObservedLogs();
    }, delay);
  }

  function stopLogTimer() {
    if (app.logTimer !== null) {
      window.clearTimeout(app.logTimer);
      app.logTimer = null;
    }
  }

  function scheduleClockTick() {
    if (app.clockTimer !== null) {
      window.clearTimeout(app.clockTimer);
    }
    app.clockTimer = window.setTimeout(() => {
      app.clockTimer = null;
      if (app.state && !elements["console-workspace"].hidden) {
        renderTelemetryAge();
        renderLongform();
        renderOverallFacts();
        const stale = app.lastResponseAt !== null
          && Date.now() - app.lastResponseAt > CONNECTION_STALE_MS;
        if (stale && app.stateFresh) {
          app.stateFresh = false;
          app.connectionError = "The last validated console response is stale.";
          setConnection("Stale", "danger");
          renderControls();
          renderErrors();
        }
      }
      scheduleClockTick();
    }, 1_000);
  }

  async function postJson(path, body) {
    if (!app.state || !app.stateFresh) {
      throw new Error("No current pipeline state is loaded.");
    }
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), MUTATION_REQUEST_TIMEOUT_MS);
    try {
      return await requestJson(path, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-HIMR-CSRF": app.state.csrf_token,
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
    } finally {
      window.clearTimeout(timeout);
    }
  }

  async function requestJson(path, options) {
    const response = await fetch(path, {
      ...options,
      headers: { Accept: "application/json", ...(options.headers || {}) },
      credentials: "same-origin",
      cache: "no-store",
      redirect: "error",
    });
    let payload = null;
    try {
      payload = await readJsonResponse(response);
    } catch (error) {
      if (response.ok) {
        throw error;
      }
    }
    if (!response.ok) {
      const message = isPlainObject(payload) && typeof payload.message === "string"
        ? payload.message
        : isPlainObject(payload) && typeof payload.error === "string"
          ? payload.error
          : `Request returned HTTP ${response.status}.`;
      throw new HttpError(response.status, message, payload);
    }
    return payload;
  }

  async function readJsonResponse(response, maximum = MAX_RESPONSE_CHARS) {
    const contentType = response.headers.get("Content-Type") || "";
    if (!contentType.toLowerCase().startsWith("application/json")) {
      throw new Error("Server response is not JSON.");
    }
    const text = await readBoundedText(response, maximum);
    try {
      return JSON.parse(text);
    } catch (_error) {
      throw new Error("Server returned malformed JSON.");
    }
  }

  async function readBoundedText(response, maximum) {
    const declared = response.headers.get("Content-Length");
    if (declared !== null && /^\d+$/.test(declared) && Number(declared) > maximum) {
      throw new Error("Server response exceeds the client byte bound.");
    }
    const text = await response.text();
    if (text.length > maximum) {
      throw new Error("Server response exceeds the client character bound.");
    }
    return text;
  }

  class HttpError extends Error {
    constructor(status, message, payload) {
      super(message);
      this.name = "HttpError";
      this.status = status;
      this.payload = payload;
    }
  }

  function readAxes(value) {
    if (!isPlainObject(value)) {
      return { process: null, durable: null, gate: null };
    }
    const nested = isPlainObject(value.state_axes)
      ? value.state_axes
      : isPlainObject(value.axes)
        ? value.axes
        : {};
    return {
      process: axisValue(value, nested, "process"),
      durable: axisValue(value, nested, "durable"),
      gate: axisValue(value, nested, "gate"),
    };
  }

  function axisValue(root, nested, name) {
    const candidate = nested[name]
      ?? root[`${name}_state`]
      ?? root[`${name}_status`]
      ?? null;
    if (typeof candidate === "string") {
      return optionalText(candidate, 200);
    }
    if (isPlainObject(candidate)) {
      return optionalText(candidate.label ?? candidate.state ?? candidate.status, 200);
    }
    return null;
  }

  function hasAxes(axes) {
    return Boolean(axes.process || axes.durable || axes.gate);
  }

  function renderFacts(container, facts) {
    container.replaceChildren();
    for (const [label, value, code] of facts) {
      const row = document.createElement("div");
      const term = document.createElement("dt");
      const description = document.createElement("dd");
      term.textContent = label;
      if (code) {
        const codeElement = document.createElement("code");
        codeElement.textContent = String(value);
        description.append(codeElement);
      } else {
        description.textContent = String(value);
      }
      row.append(term, description);
      container.append(row);
    }
  }

  function showOnlyRootPanel(id) {
    for (const panelId of ["loading-panel", "invalid-panel", "console-workspace"]) {
      elements[panelId].hidden = panelId !== id;
    }
  }

  function failClosed(message) {
    app.stateFresh = false;
    app.connectionError = message;
    app.logAbort?.abort();
    stopLogTimer();
    elements["start-pipeline"].disabled = true;
    elements["stop-pipeline"].disabled = true;
    elements["invalid-detail"].textContent = message;
    showOnlyRootPanel("invalid-panel");
    setConnection("Unavailable", "danger");
    showErrors([message]);
    elements["invalid-panel"].focus();
    scheduleStateRefresh(5_000);
  }

  function showErrors(errors) {
    const list = elements["error-list"];
    list.replaceChildren();
    for (const message of errors) {
      const item = document.createElement("li");
      item.textContent = message;
      list.append(item);
    }
    elements["error-summary"].hidden = false;
    elements["error-summary"].focus();
  }

  function clearErrorSummary() {
    elements["error-summary"].hidden = true;
    elements["error-list"].replaceChildren();
  }

  function setConnection(label, tone) {
    setBadge(elements["connection-badge"], label, tone);
  }

  function setBadge(element, label, tone) {
    element.textContent = label;
    element.dataset.tone = tone;
  }

  function announce(message) {
    elements["aggregate-status"].textContent = message;
  }

  function toneForState(value) {
    const state = normalizeStateText(value);
    if (SUCCESS_STATES.has(state) || ["progressed", "skipped"].includes(state)) {
      return "success";
    }
    if (
      ACTIVE_STATES.has(state)
      || ["at_capacity", "bounded", "held", "recorded", "reserved", "review_required", "warning"].includes(state)
    ) {
      return "warning";
    }
    if (DANGER_STATES.has(state)) {
      return "danger";
    }
    return "neutral";
  }

  function normalizeStateText(value) {
    return typeof value === "string"
      ? value.trim().toLowerCase().replaceAll(" ", "_").replaceAll("-", "_")
      : "";
  }

  function formatTime(value) {
    if (!value) {
      return null;
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return value;
    }
    return date.toLocaleString();
  }

  function formatElapsed(startValue, endValue) {
    const start = Date.parse(startValue);
    const end = endValue ? Date.parse(endValue) : Date.now();
    if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
      return "Not available";
    }
    return formatDurationMilliseconds(end - start);
  }

  function formatDurationMilliseconds(milliseconds) {
    const seconds = Math.max(0, Math.round(milliseconds / 1_000));
    const hours = Math.floor(seconds / 3_600);
    const minutes = Math.floor((seconds % 3_600) / 60);
    const remainder = seconds % 60;
    if (hours > 0) {
      return `${hours}h ${minutes}m`;
    }
    if (minutes > 0) {
      return `${minutes}m ${remainder}s`;
    }
    return `${remainder}s`;
  }

  function formatBytes(value) {
    if (!Number.isFinite(value) || value < 0) {
      return "Not reported";
    }
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let amount = value;
    let unit = 0;
    while (amount >= 1024 && unit < units.length - 1) {
      amount /= 1024;
      unit += 1;
    }
    return `${formatNumber(amount, unit === 0 ? 0 : amount >= 10 ? 1 : 2)} ${units[unit]}`;
  }

  function formatNumber(value, maximumFractionDigits) {
    return value.toLocaleString("en-US", { maximumFractionDigits });
  }

  function titleCase(value) {
    if (typeof value !== "string" || value.length === 0) {
      return "Not reported";
    }
    return value
      .replaceAll("_", " ")
      .replaceAll("-", " ")
      .replace(/\b\w/g, (character) => character.toUpperCase())
      .replace(/\bAsr\b/g, "ASR")
      .replace(/\bGpu\b/g, "GPU")
      .replace(/\bEta\b/g, "ETA")
      .replace(/\bId\b/g, "ID");
  }

  function compactJobId(value) {
    return value.length <= 20 ? value : `${value.slice(0, 8)}…${value.slice(-8)}`;
  }

  function readPath(root, path) {
    let current = root;
    for (const part of path) {
      if (!isPlainObject(current) || !(part in current)) {
        return undefined;
      }
      current = current[part];
    }
    return current;
  }

  function firstText(root, paths, maximum) {
    if (!isPlainObject(root)) {
      return null;
    }
    for (const path of paths) {
      const value = optionalText(readPath(root, path), maximum);
      if (value) {
        return value;
      }
    }
    return null;
  }

  function firstFiniteNumber(value, keys) {
    for (const key of keys) {
      if (typeof value[key] === "number" && Number.isFinite(value[key])) {
        return value[key];
      }
    }
    return null;
  }

  function firstNumberAtPaths(root, paths) {
    if (!isPlainObject(root)) {
      return null;
    }
    for (const path of paths) {
      const value = readPath(root, path);
      if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
        return value;
      }
    }
    return null;
  }

  function safeCount(value) {
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
  }

  function positiveIntegerOrNull(value) {
    return Number.isSafeInteger(value) && value > 0 ? value : null;
  }

  function nonnegativeIntegerOrNull(value) {
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
  }

  function boundedText(value, label, maximum) {
    if (typeof value !== "string" || value.length < 1 || value.length > maximum) {
      throw new Error(`${label} must be bounded non-empty text.`);
    }
    return value;
  }

  function optionalText(value, maximum) {
    if (value === null || value === undefined || value === "") {
      return null;
    }
    return typeof value === "string" && value.length <= maximum ? value : null;
  }

  function hasControlCharacter(value) {
    return Array.from(value).some((character) => character.codePointAt(0) < 32 && character !== "\t");
  }

  function isPlainObject(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }

  function errorMessage(error, fallback = "The request could not be completed.") {
    return error instanceof Error && error.message ? error.message : fallback;
  }
})();
