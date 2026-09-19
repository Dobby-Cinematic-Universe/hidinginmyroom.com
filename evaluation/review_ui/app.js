"use strict";

(() => {
  const API = Object.freeze({
    bootstrap: "./api/bootstrap",
    mutate: "./api/mutate",
    finalize: "./api/finalize",
  });

  const STATES = new Set(["editing", "ready", "completed", "invalid"]);
  const SPLITS = new Set(["calibration", "scoring"]);
  const TRIM_REASONS = new Set([
    "remove_non_speech_edge",
    "remove_sensitive_edge",
    "speech_boundary_refinement",
    "other_reviewed_trim",
  ]);
  const REJECTION_REASONS = new Set([
    "boundary_requires_reproposal",
    "duplicate_or_redundant",
    "no_usable_speech",
    "out_of_scope",
    "playback_only",
    "privacy_or_sensitivity",
    "technical_quality",
    "other_reviewed_reason",
  ]);
  const NOISE_LEVELS = new Set(["clean", "light", "moderate", "heavy", "unknown"]);
  const LANGUAGE_TAG_PATTERN = /^(?:[a-z]{2,3}|und)(?:-[A-Za-z0-9]{2,8})*$/;
  const REVIEWER_ID_PATTERN = /^[A-Za-z][A-Za-z0-9._:-]{2,127}$/;
  const SYSTEM_ID_PATTERN = /^(?:selection_|proposal_|media_|rec_|src_|rnd_|request_|review_)/i;
  const SHA256_PATTERN = /^[0-9a-f]{64}$/i;
  const FALLBACK_MINIMUM_ACCEPTED_MS = 3_600_000;
  const CONTEXT_PADDING_MS = 15_000;
  const COVERAGE_MINIMUM_MS = 100;
  const COVERAGE_MAX_SAMPLE_GAP_MS = 2_000;

  const app = {
    state: null,
    revision: null,
    csrfToken: "",
    recordings: [],
    progress: {},
    completedDigest: null,
    recordingIndex: 0,
    intervalIndex: 0,
    reviewingReady: false,
    attestationPresented: false,
    decisionDirty: false,
    splitDirty: false,
    pendingMutations: 0,
    mutationTail: Promise.resolve(),
    activeSegment: null,
    loadedMediaUrl: null,
    lastPlaybackMs: null,
    coverageDraft: null,
    deferredCoverage: [],
    saveUncertain: false,
    bootstrapped: false,
  };

  const elements = {};

  document.addEventListener("DOMContentLoaded", initialize);

  function initialize() {
    collectElements();
    bindEvents();
    loadBootstrap({ showLoading: true, preserveSelection: false }).catch((error) => {
      failClosed(error instanceof Error ? error.message : "The review could not be loaded.");
    });
  }

  function collectElements() {
    const ids = [
      "main-content", "state-badge", "connection-status", "error-summary", "error-list",
      "loading-panel", "loading-detail", "invalid-panel", "invalid-title", "invalid-detail", "invalid-reload", "completed-panel",
      "completed-digest", "attestation-panel", "attestation-form", "attestation-progress",
      "attestation-title", "attestation-intro",
      "reviewer-id", "attest-parent-media", "attest-no-asr", "attest-no-reference",
      "attest-selection-basis", "return-to-review", "finalize-review", "attestation-save-status",
      "review-workspace", "recording-count", "progress-label", "progress-percent",
      "decision-progress", "accepted-duration", "proposal-duration", "coverage-progress", "removal-budget",
      "continue-attestation", "recording-list", "media-title", "media-verified", "recording-title",
      "recording-native-id", "recording-split-badge", "parent-video", "media-status",
      "play-proposal", "play-context", "play-accepted", "seek-back", "seek-forward",
      "loop-segment", "playback-rate", "previous-interval", "next-interval", "interval-strip",
      "interval-status-badge", "proposal-start", "proposal-end", "interval-duration",
      "accepted-range", "playback-coverage", "save-indicator", "decision-form", "split-fieldset",
      "split-calibration", "split-scoring", "save-split", "clear-split", "decision-fieldset",
      "decision-full", "decision-trim", "decision-exclude", "trim-fields", "include-fields",
      "exclude-fields", "accepted-start", "accepted-end", "adjustment-reason", "language-tags",
      "noise-level", "rejection-reason", "clear-decision", "save-decision", "code-switch-fieldset",
      "speaker-overlap-fieldset", "playback-speech-fieldset",
    ];

    for (const id of ids) {
      const element = document.getElementById(id);
      if (!element) {
        throw new Error(`Required interface element is unavailable: ${id}`);
      }
      elements[id] = element;
    }
  }

  function bindEvents() {
    elements["invalid-reload"].addEventListener("click", () => {
      loadBootstrap({ showLoading: true, preserveSelection: true }).catch((error) => {
        failClosed(error instanceof Error ? error.message : "Reload failed.");
      });
    });

    elements["decision-form"].addEventListener("submit", saveDecision);
    elements["decision-form"].addEventListener("input", markFormDirty);
    elements["decision-form"].addEventListener("change", (event) => {
      markFormDirty(event);
      if (event.target instanceof HTMLInputElement && event.target.name === "decision_mode") {
        updateDecisionFieldVisibility();
      }
    });
    elements["save-split"].addEventListener("click", saveSplit);
    elements["clear-split"].addEventListener("click", clearSplit);
    elements["clear-decision"].addEventListener("click", clearDecision);

    elements["play-proposal"].addEventListener("click", () => playNamedSegment("proposal"));
    elements["play-context"].addEventListener("click", () => playNamedSegment("context"));
    elements["play-accepted"].addEventListener("click", () => playNamedSegment("accepted"));
    elements["seek-back"].addEventListener("click", () => seekBy(-5));
    elements["seek-forward"].addEventListener("click", () => seekBy(5));
    elements["playback-rate"].addEventListener("change", () => {
      const value = Number(elements["playback-rate"].value);
      if (Number.isFinite(value) && value >= 0.5 && value <= 2) {
        elements["parent-video"].playbackRate = value;
        announceMedia(`Playback rate ${value} times.`);
      }
    });
    elements["loop-segment"].addEventListener("change", () => {
      announceMedia(elements["loop-segment"].checked ? "Segment loop enabled." : "Segment loop disabled.");
    });
    elements["previous-interval"].addEventListener("click", () => selectRelativeInterval(-1));
    elements["next-interval"].addEventListener("click", () => selectRelativeInterval(1));

    const video = elements["parent-video"];
    video.addEventListener("loadedmetadata", handleMediaMetadata);
    video.addEventListener("durationchange", handleMediaMetadata);
    video.addEventListener("play", () => {
      app.lastPlaybackMs = safeCurrentMediaMs();
      announceMedia("Playing direct parent media.");
    });
    video.addEventListener("timeupdate", handlePlaybackProgress);
    video.addEventListener("seeking", () => {
      flushCoverage();
      app.lastPlaybackMs = null;
    });
    video.addEventListener("seeked", () => {
      app.lastPlaybackMs = safeCurrentMediaMs();
    });
    video.addEventListener("pause", () => {
      capturePlaybackSample(true);
      flushCoverage();
      if (!video.ended) {
        announceMedia(`Paused at ${formatMediaClock(safeCurrentMediaMs())}.`);
      }
    });
    video.addEventListener("ended", () => {
      capturePlaybackSample(true);
      flushCoverage();
      announceMedia("Parent media ended.");
    });
    video.addEventListener("error", () => {
      app.loadedMediaUrl = null;
      elements["media-verified"].textContent = "Parent media playback failed";
      elements["media-verified"].classList.remove("badge-success");
      updateMutationControls();
      announceMedia("The direct parent media could not be played. Decisions remain unsaved.", true);
    });

    elements["return-to-review"].addEventListener("click", () => {
      app.reviewingReady = true;
      renderApp();
      elements["review-workspace"].focus?.();
    });
    elements["continue-attestation"].addEventListener("click", presentAttestation);
    elements["attestation-form"].addEventListener("submit", finalizeReview);

    document.addEventListener("keydown", handleKeyboardShortcut);
    window.addEventListener("beforeunload", (event) => {
      if (
        app.decisionDirty
        || app.splitDirty
        || app.pendingMutations > 0
        || app.coverageDraft !== null
        || app.deferredCoverage.length > 0
        || app.saveUncertain
      ) {
        event.preventDefault();
        event.returnValue = "";
      }
    });
  }

  async function loadBootstrap({ showLoading = false, preserveSelection = true, renderResult = true } = {}) {
    const previousRecordingId = preserveSelection ? currentRecording()?.recording_id : null;
    const previousDecisionId = preserveSelection ? currentInterval()?.selection_decision_id : null;

    if (showLoading) {
      showOnlyPanel("loading-panel");
      elements["loading-detail"].textContent = app.bootstrapped
        ? "Refreshing the current server revision…"
        : "Requesting the current same-session draft…";
    }
    setConnectionStatus("Loading…");

    const response = await fetch(API.bootstrap, {
      method: "GET",
      credentials: "same-origin",
      cache: "no-store",
      redirect: "error",
      headers: { Accept: "application/json" },
    });
    const payload = await readJson(response);
    if (!response.ok) {
      throw new Error(safeServerMessage(payload, "The server rejected the bootstrap request."));
    }

    adoptBootstrap(payload, { previousRecordingId, previousDecisionId });
    app.deferredCoverage = app.deferredCoverage.filter((operation) => !coverageOperationPresent(operation));
    app.saveUncertain = false;
    app.bootstrapped = true;
    setConnectionStatus("Current revision loaded");
    if (renderResult) {
      clearErrors();
      renderApp();
    }
    if (app.deferredCoverage.length > 0) {
      setConnectionStatus("Playback coverage is still unsaved; replay or navigate to retry it.");
    }
  }

  function adoptBootstrap(payload, selection = {}) {
    validateBootstrap(payload);
    app.state = payload.state;
    app.revision = payload.revision;
    app.csrfToken = payload.csrf_token;
    app.recordings = payload.recordings;
    app.progress = isPlainObject(payload.progress) ? payload.progress : {};
    app.completedDigest = typeof payload.completed_manifest_sha256 === "string"
      ? payload.completed_manifest_sha256
      : null;

    const previousRecordingId = selection.previousRecordingId;
    const previousDecisionId = selection.previousDecisionId;
    let recordingIndex = previousRecordingId === null || previousRecordingId === undefined
      ? app.recordingIndex
      : app.recordings.findIndex((recording) => recording.recording_id === previousRecordingId);
    if (!Number.isInteger(recordingIndex) || recordingIndex < 0 || recordingIndex >= app.recordings.length) {
      recordingIndex = 0;
    }
    app.recordingIndex = recordingIndex;

    const intervals = currentRecording()?.intervals ?? [];
    let intervalIndex = previousDecisionId === null || previousDecisionId === undefined
      ? app.intervalIndex
      : intervals.findIndex((interval) => interval.selection_decision_id === previousDecisionId);
    if (!Number.isInteger(intervalIndex) || intervalIndex < 0 || intervalIndex >= intervals.length) {
      intervalIndex = 0;
    }
    app.intervalIndex = intervalIndex;
  }

  function validateBootstrap(payload) {
    if (!isPlainObject(payload)) {
      throw new Error("The server returned an invalid bootstrap document.");
    }
    if (!STATES.has(payload.state)) {
      throw new Error("The server returned an unknown review state. Editing is disabled.");
    }
    if (!Number.isInteger(payload.revision) || payload.revision < 0) {
      throw new Error("The server did not return a usable revision.");
    }
    if (typeof payload.csrf_token !== "string" || payload.csrf_token.length < 1) {
      throw new Error("The same-session save token is unavailable.");
    }
    if (!Array.isArray(payload.recordings)) {
      throw new Error("The server did not return a recording list.");
    }
    for (const recording of payload.recordings) {
      if (!isPlainObject(recording) || typeof recording.recording_id !== "string" || !Array.isArray(recording.intervals)) {
        throw new Error("The server returned an invalid recording entry.");
      }
      if (recording.split !== null && !SPLITS.has(recording.split)) {
        throw new Error("The server returned an invalid recording split.");
      }
      for (const interval of recording.intervals) {
        if (
          !isPlainObject(interval)
          || typeof interval.selection_decision_id !== "string"
          || !Number.isInteger(interval.proposal_start_ms)
          || !Number.isInteger(interval.proposal_end_ms)
          || interval.proposal_start_ms < 0
          || interval.proposal_end_ms <= interval.proposal_start_ms
        ) {
          throw new Error("The server returned invalid parent interval coordinates.");
        }
      }
    }
  }

  function renderApp() {
    hideAllPanels();
    setStateBadge();

    if (app.saveUncertain) {
      preserveCoverageDraft();
      stopMediaWithoutSaving();
      elements["invalid-title"].textContent = "Save state could not be verified";
      elements["invalid-detail"].textContent = "A network failure interrupted revision reconciliation. Editing and finalization are disabled until the current server revision is reloaded. Recent playback coverage may need to be replayed.";
      elements["invalid-panel"].hidden = false;
      return;
    }

    if (app.state === "completed") {
      stopMediaWithoutSaving();
      elements["completed-panel"].hidden = false;
      elements["completed-digest"].textContent = validDigest(app.completedDigest)
        ? app.completedDigest
        : "Not returned";
      return;
    }

    if (app.state === "invalid") {
      stopMediaWithoutSaving();
      elements["invalid-title"].textContent = "This draft is invalid";
      elements["invalid-detail"].textContent = "The server could not establish a safe review state. No decisions can be changed or finalized.";
      elements["invalid-panel"].hidden = false;
      return;
    }

    if (isFinalizationRecovery()) {
      stopMediaWithoutSaving();
      if (!app.attestationPresented) {
        resetAttestationForm();
        app.attestationPresented = true;
      }
      configureAttestationScreen(true);
      renderAttestationSummary();
      elements["attestation-panel"].hidden = false;
      return;
    }

    if (app.state === "ready" && !app.reviewingReady) {
      if (!elements["parent-video"].paused) {
        elements["parent-video"].pause();
      }
      if (!app.attestationPresented) {
        resetAttestationForm();
        app.attestationPresented = true;
      }
      configureAttestationScreen(false);
      renderAttestationSummary();
      elements["attestation-panel"].hidden = false;
      return;
    }

    elements["review-workspace"].hidden = false;
    renderWorkspace();
  }

  function hideAllPanels() {
    for (const id of ["loading-panel", "invalid-panel", "completed-panel", "attestation-panel", "review-workspace"]) {
      elements[id].hidden = true;
    }
  }

  function showOnlyPanel(id) {
    hideAllPanels();
    elements[id].hidden = false;
  }

  function setStateBadge() {
    const badge = elements["state-badge"];
    badge.className = "state-badge";
    if (app.saveUncertain) {
      badge.classList.add("state-invalid");
      badge.textContent = "SAVE STATE UNKNOWN — reload required";
    } else if (app.state === "completed") {
      badge.classList.add("state-completed");
      badge.textContent = "COMPLETED — read only";
    } else if (app.state === "invalid") {
      badge.classList.add("state-invalid");
      badge.textContent = "INVALID DRAFT — editing disabled";
    } else if (isFinalizationRecovery()) {
      badge.classList.add("state-draft");
      badge.textContent = "DRAFT FINALIZATION — recovery required; editing disabled";
    } else if (app.state === "ready") {
      badge.classList.add("state-draft");
      badge.textContent = "DRAFT — not valid or completed; ready to attest";
    } else {
      badge.classList.add("state-draft");
      badge.textContent = "DRAFT — not valid or completed";
    }
  }

  function renderWorkspace() {
    const activeId = document.activeElement instanceof HTMLElement ? document.activeElement.id : "";
    renderProgress();
    renderRecordingList();
    renderRecordingContext();
    renderIntervalStrip();
    renderCoordinates();
    fillDecisionForm();
    updateMutationControls();
    elements["continue-attestation"].hidden = app.state !== "ready";

    if (activeId) {
      const replacement = document.getElementById(activeId);
      if (replacement instanceof HTMLElement && !replacement.hidden && !replacement.closest("[hidden]")) {
        replacement.focus({ preventScroll: true });
      }
    }
  }

  function renderProgress() {
    const metrics = deriveProgress();
    const percent = metrics.intervalCount > 0
      ? Math.round((metrics.decidedCount / metrics.intervalCount) * 100)
      : 0;
    elements["recording-count"].textContent = `${metrics.recordingCount} recording${metrics.recordingCount === 1 ? "" : "s"}`;
    elements["progress-label"].textContent = `${metrics.decidedCount} of ${metrics.intervalCount} intervals`;
    elements["progress-percent"].textContent = `${percent}%`;
    elements["decision-progress"].max = Math.max(1, metrics.intervalCount);
    elements["decision-progress"].value = metrics.decidedCount;
    elements["decision-progress"].textContent = `${percent}%`;
    const shortfallDetail = metrics.durationShortfallMs > 0
      ? ` · ${formatDuration(metrics.durationShortfallMs)} short`
      : "";
    elements["accepted-duration"].textContent = `${formatDuration(metrics.acceptedDurationMs)} / ${formatDuration(metrics.minimumAcceptedMs)}${shortfallDetail}`;
    elements["proposal-duration"].textContent = formatDuration(metrics.proposalDurationMs);
    elements["coverage-progress"].textContent = `${metrics.coveredIntervalCount} of ${metrics.intervalCount} intervals · ${formatDuration(metrics.coverageDurationMs)} observed`;

    elements["removal-budget"].classList.toggle("text-danger", metrics.completionImpossible);
    if (metrics.completionImpossible) {
      const impossibleShortfall = Math.max(0, metrics.minimumAcceptedMs - metrics.maximumPossibleDurationMs);
      elements["removal-budget"].textContent = `Over budget: even all pending proposals leave ${formatDuration(impossibleShortfall)} short (maximum ${formatDuration(metrics.maximumPossibleDurationMs)})`;
    } else if (metrics.removalRemainingMs >= 0) {
      elements["removal-budget"].textContent = `${formatDuration(metrics.removalRemainingMs)} remaining (${formatDuration(metrics.removedDurationMs)} used)`;
    } else {
      elements["removal-budget"].textContent = `${formatDuration(Math.abs(metrics.removalRemainingMs))} over budget`;
    }
  }

  function deriveProgress() {
    let intervalCount = 0;
    let decidedCount = 0;
    let proposalDurationMs = 0;
    let acceptedDurationMs = 0;
    let removedDurationMs = 0;
    let coverageDurationMs = 0;
    let coveredIntervalCount = 0;
    let pendingFullDurationMs = 0;

    for (const recording of app.recordings) {
      for (const interval of recording.intervals) {
        const duration = interval.proposal_end_ms - interval.proposal_start_ms;
        intervalCount += 1;
        proposalDurationMs += duration;
        const intervalCoverage = Number(interval.playback_coverage_ms);
        if (Number.isFinite(intervalCoverage) && intervalCoverage > 0) {
          coverageDurationMs += intervalCoverage;
          const requiredCoverage = Number.isInteger(interval.required_playback_coverage_ms)
            ? interval.required_playback_coverage_ms
            : Math.max(1, duration - 1_000);
          if (intervalCoverage >= requiredCoverage) {
            coveredIntervalCount += 1;
          }
        }
        if (interval.decision === "include") {
          decidedCount += 1;
          const acceptedStart = Number(interval.accepted_start_ms);
          const acceptedEnd = Number(interval.accepted_end_ms);
          if (Number.isInteger(acceptedStart) && Number.isInteger(acceptedEnd) && acceptedEnd > acceptedStart) {
            acceptedDurationMs += acceptedEnd - acceptedStart;
            removedDurationMs += duration - (acceptedEnd - acceptedStart);
          }
        } else if (interval.decision === "exclude") {
          decidedCount += 1;
          removedDurationMs += duration;
        } else {
          pendingFullDurationMs += duration;
        }
      }
    }

    const serverProgress = app.progress;
    intervalCount = numericProgress(serverProgress, ["interval_count", "total_intervals"], intervalCount);
    decidedCount = numericProgress(serverProgress, ["decided_interval_count", "completed_intervals", "intervals_completed"], decidedCount);
    proposalDurationMs = numericProgress(serverProgress, ["proposal_duration_ms", "total_duration_ms"], proposalDurationMs);
    acceptedDurationMs = numericProgress(serverProgress, ["accepted_duration_ms"], acceptedDurationMs);
    removedDurationMs = numericProgress(serverProgress, ["removed_duration_ms"], removedDurationMs);
    coverageDurationMs = numericProgress(serverProgress, ["coverage_duration_ms"], coverageDurationMs);
    if (Array.isArray(serverProgress.missing_coverage_selection_decision_ids)) {
      coveredIntervalCount = Math.max(0, intervalCount - serverProgress.missing_coverage_selection_decision_ids.length);
    }
    const minimumAcceptedMs = numericProgress(
      serverProgress,
      ["minimum_accepted_duration_ms", "required_accepted_duration_ms"],
      FALLBACK_MINIMUM_ACCEPTED_MS,
    );
    const durationShortfallMs = numericProgress(
      serverProgress,
      ["duration_shortfall_ms"],
      Math.max(0, minimumAcceptedMs - acceptedDurationMs),
    );
    const maximumPossibleDurationMs = numericProgress(
      serverProgress,
      ["maximum_possible_duration_ms"],
      acceptedDurationMs + pendingFullDurationMs,
    );
    const completionImpossible = serverProgress.completion_impossible === true
      || maximumPossibleDurationMs < minimumAcceptedMs;
    const removalBudgetMs = numericProgress(
      serverProgress,
      ["removal_budget_ms"],
      Math.max(0, proposalDurationMs - minimumAcceptedMs),
    );
    const removalRemainingMs = numericProgress(
      serverProgress,
      ["removal_budget_remaining_ms", "remaining_removal_budget_ms"],
      removalBudgetMs - removedDurationMs,
      true,
    );

    return {
      recordingCount: app.recordings.length,
      intervalCount,
      decidedCount,
      proposalDurationMs,
      acceptedDurationMs,
      removedDurationMs,
      coverageDurationMs,
      coveredIntervalCount,
      minimumAcceptedMs,
      durationShortfallMs,
      maximumPossibleDurationMs,
      completionImpossible,
      removalRemainingMs,
    };
  }

  function numericProgress(source, keys, fallback, allowNegative = false) {
    for (const key of keys) {
      const value = source[key];
      if (Number.isFinite(value) && (allowNegative || value >= 0)) {
        return value;
      }
    }
    return fallback;
  }

  function renderRecordingList() {
    const list = elements["recording-list"];
    list.replaceChildren();

    app.recordings.forEach((recording, index) => {
      const item = document.createElement("li");
      const button = document.createElement("button");
      button.type = "button";
      button.className = "recording-item-button";
      button.id = `recording-choice-${index}`;
      button.dataset.recordingIndex = String(index);
      button.setAttribute("aria-current", index === app.recordingIndex ? "true" : "false");

      const top = document.createElement("span");
      top.className = "recording-item-top";
      const title = document.createElement("span");
      title.className = "recording-item-title";
      title.textContent = displayRecordingTitle(recording, index);
      const state = document.createElement("span");
      state.textContent = recordingStateLabel(recording);
      top.append(title, state);

      const bottom = document.createElement("span");
      bottom.className = "recording-item-bottom";
      const count = document.createElement("span");
      const decided = recording.intervals.filter((interval) => interval.decision === "include" || interval.decision === "exclude").length;
      count.textContent = `${decided}/${recording.intervals.length} decided`;
      const split = document.createElement("span");
      split.textContent = recording.split ? titleCase(recording.split) : "Split unset";
      bottom.append(count, split);

      button.append(top, bottom);
      button.setAttribute("aria-label", `${displayRecordingTitle(recording, index)}. ${decided} of ${recording.intervals.length} intervals decided. ${split.textContent}.`);
      button.addEventListener("click", () => selectRecording(index));
      item.append(button);
      list.append(item);
    });
  }

  function recordingStateLabel(recording) {
    const allDecided = recording.intervals.length > 0 && recording.intervals.every(
      (interval) => interval.decision === "include" || interval.decision === "exclude",
    );
    const hasInclude = recording.intervals.some((interval) => interval.decision === "include");
    const allCovered = recording.intervals.every(intervalCoverageComplete);
    if (allDecided && hasInclude && SPLITS.has(recording.split) && allCovered) {
      return "Complete";
    }
    if (allDecided && hasInclude && SPLITS.has(recording.split) && !allCovered) {
      return "Needs playback";
    }
    if (recording.intervals.some((interval) => interval.decision === "include" || interval.decision === "exclude")) {
      return "In progress";
    }
    return "Open";
  }

  function renderRecordingContext() {
    const recording = currentRecording();
    if (!recording) {
      elements["recording-title"].textContent = "No recording selected";
      elements["recording-native-id"].textContent = "";
      elements["recording-split-badge"].textContent = "Split unset";
      clearMedia();
      return;
    }

    elements["recording-title"].textContent = displayRecordingTitle(recording, app.recordingIndex);
    const metadata = [];
    if (typeof recording.source_native_id === "string" && recording.source_native_id) {
      metadata.push(`Source native ID: ${recording.source_native_id}`);
    }
    if (Number.isInteger(recording.proposal_schema_version)) {
      metadata.push(`Proposal schema v${recording.proposal_schema_version}`);
    }
    if (typeof recording.media?.mime_type === "string" && recording.media.mime_type) {
      metadata.push(recording.media.mime_type);
    }
    const mediaDuration = Number(recording.media?.duration_ms);
    if (Number.isFinite(mediaDuration) && mediaDuration >= 0) {
      metadata.push(`Media ${formatDuration(mediaDuration)}`);
    }
    const byteCount = Number(recording.media?.byte_count);
    if (Number.isFinite(byteCount) && byteCount >= 0) {
      metadata.push(formatBytes(byteCount));
    }
    elements["recording-native-id"].textContent = metadata.join(" · ");
    elements["recording-split-badge"].textContent = recording.split
      ? `${titleCase(recording.split)} split`
      : "Split unset";

    const verified = recording.media?.verified === true;
    elements["media-verified"].textContent = verified ? "Server-verified parent media" : "Media not verified";
    elements["media-verified"].classList.toggle("badge-success", verified);
    loadRecordingMedia(recording);
  }

  function loadRecordingMedia(recording) {
    const video = elements["parent-video"];
    if (recording.media?.verified !== true) {
      clearMedia();
      announceMedia("Playback is blocked because the server did not verify this parent media.", true);
      return;
    }

    const safeUrl = validateMediaUrl(recording.media?.url);
    if (!safeUrl) {
      clearMedia();
      announceMedia("Playback is blocked because the server returned an unsafe media URL.", true);
      return;
    }

    if (app.loadedMediaUrl !== safeUrl) {
      flushCoverage();
      app.activeSegment = null;
      app.lastPlaybackMs = null;
      video.pause();
      video.removeAttribute("src");
      video.load();
      video.src = safeUrl;
      video.load();
      app.loadedMediaUrl = safeUrl;
      announceMedia("Loading server-verified direct parent media…");
    }
  }

  function validateMediaUrl(rawUrl) {
    if (typeof rawUrl !== "string" || rawUrl.length < 1 || rawUrl.length > 4096) {
      return null;
    }
    if (/[\\\u0000-\u001f\u007f]/.test(rawUrl)) {
      return null;
    }
    try {
      const resolved = new URL(rawUrl, document.baseURI);
      const expectedBase = new URL("./media/", document.baseURI);
      if (
        resolved.origin !== window.location.origin
        || resolved.origin !== expectedBase.origin
        || !resolved.pathname.startsWith(expectedBase.pathname)
        || resolved.username
        || resolved.password
      ) {
        return null;
      }
      return rawUrl;
    } catch {
      return null;
    }
  }

  function clearMedia() {
    const video = elements["parent-video"];
    flushCoverage();
    app.activeSegment = null;
    app.lastPlaybackMs = null;
    app.loadedMediaUrl = null;
    video.pause();
    video.removeAttribute("src");
    video.load();
  }

  function stopMediaWithoutSaving() {
    const video = elements["parent-video"];
    app.coverageDraft = null;
    app.activeSegment = null;
    app.lastPlaybackMs = null;
    app.loadedMediaUrl = null;
    video.pause();
    video.removeAttribute("src");
    video.load();
  }

  function renderIntervalStrip() {
    const strip = elements["interval-strip"];
    strip.replaceChildren();
    const intervals = currentRecording()?.intervals ?? [];

    intervals.forEach((interval, index) => {
      const item = document.createElement("span");
      item.className = "interval-strip-item";
      item.setAttribute("role", "listitem");
      const button = document.createElement("button");
      button.type = "button";
      button.className = "interval-button";
      button.id = `interval-choice-${index}`;
      if (interval.decision === "include") {
        button.classList.add("is-included");
      } else if (interval.decision === "exclude") {
        button.classList.add("is-excluded");
      }
      button.setAttribute("aria-current", index === app.intervalIndex ? "true" : "false");
      button.textContent = String(index + 1);
      const duration = interval.proposal_end_ms - interval.proposal_start_ms;
      button.setAttribute("aria-label", `Interval ${index + 1}, ${formatDuration(duration)}, ${decisionLabel(interval)}.`);
      button.addEventListener("click", () => selectInterval(index));
      item.append(button);
      strip.append(item);
    });

    elements["previous-interval"].disabled = app.intervalIndex <= 0;
    elements["next-interval"].disabled = app.intervalIndex >= intervals.length - 1;
  }

  function renderCoordinates() {
    const interval = currentInterval();
    if (!interval) {
      for (const id of ["proposal-start", "proposal-end", "interval-duration", "accepted-range"]) {
        elements[id].textContent = "—";
      }
      elements["playback-coverage"].textContent = "Not recorded";
      elements["interval-status-badge"].textContent = "Not reviewed";
      elements["play-accepted"].disabled = true;
      return;
    }

    elements["proposal-start"].textContent = formatCoordinate(interval.proposal_start_ms);
    elements["proposal-end"].textContent = formatCoordinate(interval.proposal_end_ms);
    elements["interval-duration"].textContent = formatDuration(interval.proposal_end_ms - interval.proposal_start_ms);
    if (
      interval.decision === "include"
      && Number.isInteger(interval.accepted_start_ms)
      && Number.isInteger(interval.accepted_end_ms)
    ) {
      elements["accepted-range"].textContent = `${formatCoordinate(interval.accepted_start_ms)} – ${formatCoordinate(interval.accepted_end_ms)}`;
    } else {
      elements["accepted-range"].textContent = interval.decision === "exclude" ? "Excluded" : "Not set";
    }
    elements["interval-status-badge"].textContent = intervalStatusLabel(interval);
    elements["play-accepted"].disabled = interval.decision !== "include";

    const coverageMs = Number(interval.playback_coverage_ms);
    if (Number.isFinite(coverageMs) && coverageMs > 0) {
      const duration = interval.proposal_end_ms - interval.proposal_start_ms;
      const percentage = Math.min(100, Math.round((coverageMs / duration) * 100));
      const rangeCount = Array.isArray(interval.playback_coverage_ranges)
        ? interval.playback_coverage_ranges.length
        : 0;
      const rangeDetail = rangeCount > 0
        ? ` across ${rangeCount} observed range${rangeCount === 1 ? "" : "s"}`
        : "";
      elements["playback-coverage"].textContent = `${formatDuration(coverageMs)} (${percentage}% of proposal${rangeDetail})`;
    } else {
      elements["playback-coverage"].textContent = "Not recorded";
    }
  }

  function fillDecisionForm() {
    const recording = currentRecording();
    const interval = currentInterval();
    clearFieldErrors();

    setRadioValue("recording_split", recording?.split ?? null);
    setRadioValue("decision_mode", null);
    setRadioValue("code_switch", null);
    setRadioValue("speaker_overlap", null);
    setRadioValue("playback_speech", null);
    elements["accepted-start"].value = "";
    elements["accepted-end"].value = "";
    elements["adjustment-reason"].value = "";
    elements["language-tags"].value = "";
    elements["noise-level"].value = "";
    elements["rejection-reason"].value = "";

    if (interval?.decision === "include") {
      const isFull = interval.accepted_start_ms === interval.proposal_start_ms
        && interval.accepted_end_ms === interval.proposal_end_ms
        && interval.adjustment_reason === null;
      setRadioValue("decision_mode", isFull ? "full" : "trim");
      if (!isFull) {
        elements["accepted-start"].value = integerString(interval.accepted_start_ms);
        elements["accepted-end"].value = integerString(interval.accepted_end_ms);
        elements["adjustment-reason"].value = TRIM_REASONS.has(interval.adjustment_reason)
          ? interval.adjustment_reason
          : "";
      }
      const flags = isPlainObject(interval.flags) ? interval.flags : {};
      elements["language-tags"].value = Array.isArray(flags.language_tags)
        ? flags.language_tags.join(", ")
        : "";
      setRadioValue("code_switch", typeof flags.code_switch === "boolean" ? String(flags.code_switch) : null);
      setRadioValue("speaker_overlap", typeof flags.speaker_overlap === "boolean" ? String(flags.speaker_overlap) : null);
      setRadioValue("playback_speech", typeof flags.playback_speech === "boolean" ? String(flags.playback_speech) : null);
      elements["noise-level"].value = NOISE_LEVELS.has(flags.noise) ? flags.noise : "";
    } else if (interval?.decision === "exclude") {
      setRadioValue("decision_mode", "exclude");
      elements["rejection-reason"].value = REJECTION_REASONS.has(interval.rejection_reason)
        ? interval.rejection_reason
        : "";
    }

    app.decisionDirty = false;
    app.splitDirty = false;
    updateDecisionFieldVisibility();
  }

  function updateDecisionFieldVisibility() {
    const mode = radioValue("decision_mode");
    elements["trim-fields"].hidden = mode !== "trim";
    elements["include-fields"].hidden = mode !== "full" && mode !== "trim";
    elements["exclude-fields"].hidden = mode !== "exclude";
  }

  function updateMutationControls() {
    const recordingUsable = Boolean(currentRecording() && currentInterval())
      && app.state !== "completed"
      && app.state !== "invalid";
    const reviewUsable = recordingUsable && currentMediaUsable();
    const mutationIdle = app.pendingMutations === 0 && !app.saveUncertain;
    elements["split-fieldset"].disabled = !recordingUsable;
    elements["decision-fieldset"].disabled = !reviewUsable;
    elements["save-split"].disabled = !recordingUsable || !mutationIdle;
    elements["clear-split"].disabled = !recordingUsable || !mutationIdle || currentRecording()?.split === null;
    elements["save-decision"].disabled = !reviewUsable || !mutationIdle;
    elements["clear-decision"].disabled = !recordingUsable || !mutationIdle || currentInterval()?.decision === null;
    elements["play-proposal"].disabled = !reviewUsable;
    elements["play-context"].disabled = !reviewUsable;
    elements["play-accepted"].disabled = !reviewUsable || currentInterval()?.decision !== "include";
    elements["seek-back"].disabled = app.loadedMediaUrl === null;
    elements["seek-forward"].disabled = app.loadedMediaUrl === null;
    elements["decision-form"].setAttribute("aria-busy", app.pendingMutations > 0 ? "true" : "false");
    for (const control of elements["decision-form"].querySelectorAll(
      "#trim-fields input, #trim-fields select, #include-fields input, #include-fields select, #exclude-fields select",
    )) {
      control.disabled = !reviewUsable;
    }

    const indicator = elements["save-indicator"];
    indicator.className = "save-indicator";
    if (app.pendingMutations > 0) {
      indicator.classList.add("is-saving");
      indicator.textContent = `Saving ${app.pendingMutations} change${app.pendingMutations === 1 ? "" : "s"}…`;
    } else if (currentInterval()?.decision === "include" || currentInterval()?.decision === "exclude") {
      indicator.classList.add("is-saved");
      indicator.textContent = "Saved at current revision";
    } else {
      indicator.textContent = "Not saved";
    }
  }

  function markFormDirty(event) {
    const target = event.target;
    if (!(target instanceof HTMLInputElement || target instanceof HTMLSelectElement)) {
      return;
    }
    if (target.name === "recording_split") {
      app.splitDirty = true;
    } else {
      app.decisionDirty = true;
    }
    if (app.pendingMutations === 0) {
      elements["save-indicator"].className = "save-indicator";
      elements["save-indicator"].textContent = "Unsaved choices";
    }
  }

  function selectRecording(index) {
    if (index === app.recordingIndex || index < 0 || index >= app.recordings.length) {
      return;
    }
    if (!canDiscardUnsavedChoices()) {
      return;
    }
    clearErrors();
    pauseAndFlushPlayback();
    app.recordingIndex = index;
    app.intervalIndex = 0;
    app.activeSegment = null;
    renderWorkspace();
  }

  function selectInterval(index) {
    const intervals = currentRecording()?.intervals ?? [];
    if (index === app.intervalIndex || index < 0 || index >= intervals.length) {
      return;
    }
    if (!canDiscardUnsavedChoices()) {
      return;
    }
    clearErrors();
    const restoreIntervalFocus = document.activeElement?.classList?.contains("interval-button") === true;
    pauseAndFlushPlayback();
    app.intervalIndex = index;
    app.activeSegment = null;
    renderIntervalStrip();
    renderCoordinates();
    fillDecisionForm();
    updateMutationControls();
    if (restoreIntervalFocus) {
      document.getElementById(`interval-choice-${index}`)?.focus({ preventScroll: true });
    }
  }

  function selectRelativeInterval(delta) {
    selectInterval(app.intervalIndex + delta);
  }

  function canDiscardUnsavedChoices() {
    if (!app.decisionDirty && !app.splitDirty) {
      return true;
    }
    return window.confirm("Discard the unsaved choices on this interval?");
  }

  async function saveSplit() {
    clearErrors();
    const recording = currentRecording();
    const split = radioValue("recording_split");
    if (!recording) {
      showErrors([{ id: "recording-list", message: "Select a recording before assigning a split." }]);
      return;
    }
    if (!SPLITS.has(split)) {
      showErrors([{ id: "split-fieldset", message: "Choose calibration or scoring for this recording." }]);
      return;
    }
    if (recording.split === split) {
      app.splitDirty = false;
      announceSave("That recording split is already saved.");
      return;
    }
    if (app.decisionDirty) {
      if (!window.confirm("Saving the recording split will discard the unsaved interval choices. Continue?")) {
        return;
      }
      app.decisionDirty = false;
    }

    const recordingId = recording.recording_id;
    await enqueueMutation(
      { kind: "set_split", recording_id: recordingId, split },
      {
        optimistic: () => {
          const target = findRecording(recordingId);
          if (target) target.split = split;
          app.splitDirty = false;
        },
      },
    );
  }

  async function clearSplit() {
    clearErrors();
    const recording = currentRecording();
    if (!recording || recording.split === null) {
      setRadioValue("recording_split", null);
      app.splitDirty = false;
      return;
    }
    if (app.decisionDirty) {
      if (!window.confirm("Clearing the recording split will discard the unsaved interval choices. Continue?")) {
        return;
      }
      app.decisionDirty = false;
    }
    const recordingId = recording.recording_id;
    await enqueueMutation(
      { kind: "clear_split", recording_id: recordingId },
      {
        optimistic: () => {
          const target = findRecording(recordingId);
          if (target) target.split = null;
          app.splitDirty = false;
        },
      },
    );
  }

  async function saveDecision(event) {
    event.preventDefault();
    clearErrors();
    const interval = currentInterval();
    if (!interval) {
      showErrors([{ id: "interval-strip", message: "Select an interval before saving a decision." }]);
      return;
    }
    if (app.splitDirty) {
      if (!window.confirm("Saving the interval decision will discard the unsaved recording split choice. Continue?")) {
        return;
      }
      app.splitDirty = false;
      setRadioValue("recording_split", currentRecording()?.split ?? null);
    }
    if (!currentMediaUsable()) {
      showErrors([{ id: "media-status", message: "Load server-verified direct parent media before saving an interval decision." }]);
      return;
    }

    const result = decisionOperationFromForm(interval);
    if (result.errors.length > 0) {
      showErrors(result.errors);
      return;
    }
    const decisionId = interval.selection_decision_id;
    await enqueueMutation(result.operation, {
      optimistic: () => {
        const target = findInterval(decisionId);
        if (!target) return;
        if (result.operation.kind === "set_include") {
          target.decision = "include";
          target.accepted_start_ms = result.operation.accepted_start_ms;
          target.accepted_end_ms = result.operation.accepted_end_ms;
          target.adjustment_reason = result.operation.adjustment_reason;
          target.rejection_reason = null;
          target.flags = structuredCloneSafe(result.operation.flags);
        } else {
          target.decision = "exclude";
          target.accepted_start_ms = null;
          target.accepted_end_ms = null;
          target.adjustment_reason = null;
          target.rejection_reason = result.operation.rejection_reason;
          target.flags = {
            language_tags: null,
            code_switch: null,
            speaker_overlap: null,
            playback_speech: null,
            noise: null,
          };
        }
        app.decisionDirty = false;
      },
    });
  }

  function decisionOperationFromForm(interval) {
    const mode = radioValue("decision_mode");
    const errors = [];
    if (!new Set(["full", "trim", "exclude"]).has(mode)) {
      errors.push({ id: "decision-fieldset", message: "Choose include full, include trimmed, or exclude." });
      return { operation: null, errors };
    }

    if (mode === "exclude") {
      const reason = elements["rejection-reason"].value;
      if (!REJECTION_REASONS.has(reason)) {
        errors.push({ id: "rejection-reason", message: "Choose a reviewed exclusion reason." });
      }
      return {
        operation: errors.length === 0
          ? { kind: "set_exclude", selection_decision_id: interval.selection_decision_id, rejection_reason: reason }
          : null,
        errors,
      };
    }

    let acceptedStart = interval.proposal_start_ms;
    let acceptedEnd = interval.proposal_end_ms;
    let adjustmentReason = null;
    if (mode === "trim") {
      acceptedStart = strictInteger(elements["accepted-start"].value);
      acceptedEnd = strictInteger(elements["accepted-end"].value);
      adjustmentReason = elements["adjustment-reason"].value;
      if (acceptedStart === null) {
        errors.push({ id: "accepted-start", message: "Enter the trimmed start as an integer number of milliseconds." });
      }
      if (acceptedEnd === null) {
        errors.push({ id: "accepted-end", message: "Enter the trimmed end as an integer number of milliseconds." });
      }
      if (
        acceptedStart !== null
        && acceptedEnd !== null
        && (
          acceptedStart < interval.proposal_start_ms
          || acceptedEnd > interval.proposal_end_ms
          || acceptedEnd <= acceptedStart
        )
      ) {
        errors.push({ id: "accepted-start", message: "The accepted range must be a nonempty inward trim of the exact proposal range." });
      }
      if (
        acceptedStart === interval.proposal_start_ms
        && acceptedEnd === interval.proposal_end_ms
      ) {
        errors.push({ id: "decision-full", message: "Choose include full when both accepted coordinates equal the proposal." });
      }
      if (!TRIM_REASONS.has(adjustmentReason)) {
        errors.push({ id: "adjustment-reason", message: "Choose a reviewed trim reason." });
      }
    }

    const languageResult = parseLanguageTags(elements["language-tags"].value);
    errors.push(...languageResult.errors);
    const codeSwitch = explicitBoolean("code_switch", "Code switching", errors);
    const speakerOverlap = explicitBoolean("speaker_overlap", "Speaker overlap", errors);
    const playbackSpeech = explicitBoolean("playback_speech", "Playback speech", errors);
    const noise = elements["noise-level"].value;
    if (!NOISE_LEVELS.has(noise)) {
      errors.push({ id: "noise-level", message: "Choose a noise level." });
    }
    if (codeSwitch === true && languageResult.tags.length < 2) {
      errors.push({ id: "language-tags", message: "Code switching requires at least two distinct language tags." });
    }

    return {
      operation: errors.length === 0
        ? {
            kind: "set_include",
            selection_decision_id: interval.selection_decision_id,
            accepted_start_ms: acceptedStart,
            accepted_end_ms: acceptedEnd,
            adjustment_reason: adjustmentReason,
            flags: {
              language_tags: languageResult.tags,
              code_switch: codeSwitch,
              speaker_overlap: speakerOverlap,
              playback_speech: playbackSpeech,
              noise,
            },
          }
        : null,
      errors,
    };
  }

  function parseLanguageTags(raw) {
    const tags = raw.split(/[\s,]+/).map((value) => value.trim()).filter(Boolean);
    const errors = [];
    if (tags.length === 0) {
      errors.push({ id: "language-tags", message: "Enter at least one language tag." });
      return { tags: [], errors };
    }
    const invalid = tags.filter((tag) => !LANGUAGE_TAG_PATTERN.test(tag));
    if (invalid.length > 0) {
      errors.push({ id: "language-tags", message: "Use valid BCP-47-like language tags with a lowercase language subtag." });
    }
    const unique = new Set(tags);
    if (unique.size !== tags.length) {
      errors.push({ id: "language-tags", message: "Remove duplicate language tags." });
    }
    return { tags: Array.from(unique).sort(), errors };
  }

  function explicitBoolean(name, label, errors) {
    const value = radioValue(name);
    if (value !== "true" && value !== "false") {
      const targets = {
        code_switch: "code-switch-fieldset",
        speaker_overlap: "speaker-overlap-fieldset",
        playback_speech: "playback-speech-fieldset",
      };
      errors.push({ id: targets[name], message: `Explicitly mark ${label.toLowerCase()} present or absent.` });
      return null;
    }
    return value === "true";
  }

  async function clearDecision() {
    clearErrors();
    const interval = currentInterval();
    if (!interval || interval.decision === null) {
      return;
    }
    if (app.splitDirty) {
      if (!window.confirm("Clearing the interval decision will discard the unsaved recording split choice. Continue?")) {
        return;
      }
      app.splitDirty = false;
      setRadioValue("recording_split", currentRecording()?.split ?? null);
    }
    if (!window.confirm("Clear this saved interval decision? The interval will become incomplete.")) {
      return;
    }
    const decisionId = interval.selection_decision_id;
    await enqueueMutation(
      { kind: "clear_decision", selection_decision_id: decisionId },
      {
        optimistic: () => {
          const target = findInterval(decisionId);
          if (!target) return;
          target.decision = null;
          target.accepted_start_ms = null;
          target.accepted_end_ms = null;
          target.adjustment_reason = null;
          target.rejection_reason = null;
          target.flags = {
            language_tags: null,
            code_switch: null,
            speaker_overlap: null,
            playback_speech: null,
            noise: null,
          };
          app.decisionDirty = false;
        },
      },
    );
  }

  async function enqueueMutation(operation, options = {}) {
    if (app.state === "completed" || app.state === "invalid" || isFinalizationRecovery() || app.saveUncertain) {
      showErrors([{ message: "This review is read-only and cannot accept changes." }]);
      return false;
    }
    if (!options.silent && app.pendingMutations > 0) {
      showErrors([{ message: "Wait for the pending revision save before submitting another reviewed change." }]);
      return false;
    }

    const rollbackSnapshot = snapshotReviewState();
    app.pendingMutations += 1;
    if (typeof options.optimistic === "function") {
      options.optimistic();
    }
    if (!options.silent) {
      renderApp();
      announceSave("Saving against the current server revision…");
    } else {
      updateMutationControls();
    }

    const job = async () => {
      if (app.saveUncertain) {
        if (operation.kind === "merge_coverage") {
          deferCoverageOperation(operation);
        }
        app.pendingMutations = Math.max(0, app.pendingMutations - 1);
        return false;
      }
      const stateBeforeRequest = app.state;
      let serverStateKnown = false;
      try {
        const response = await fetch(API.mutate, {
          method: "POST",
          credentials: "same-origin",
          cache: "no-store",
          redirect: "error",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
            "X-HIMR-CSRF": app.csrfToken,
          },
          body: JSON.stringify({ expected_revision: app.revision, operation }),
        });
        const payload = await readJson(response);

        if (response.status === 409 || payload?.error === "stale_revision") {
          await refreshAfterConflict({ renderResult: !options.silent });
          serverStateKnown = true;
          throw new Error("The draft changed in another request. The latest revision was loaded; review and save your choice again.");
        }
        if (!response.ok) {
          serverStateKnown = await reconcileRejectedMutation({ renderResult: !options.silent });
          throw new Error(safeServerMessage(payload, "The server rejected this change."));
        }

        if (looksLikeBootstrap(payload)) {
          const previousRecordingId = currentRecording()?.recording_id;
          const previousDecisionId = currentInterval()?.selection_decision_id;
          adoptBootstrap(payload, { previousRecordingId, previousDecisionId });
          app.deferredCoverage = app.deferredCoverage.filter((item) => !coverageOperationPresent(item));
          serverStateKnown = true;
        } else {
          await loadBootstrap({ showLoading: false, preserveSelection: true });
          serverStateKnown = true;
        }
        if (!options.silent) {
          announceSave("Saved at the current revision.");
        }
        return true;
      } catch (error) {
        if (!serverStateKnown) {
          serverStateKnown = await reconcileRejectedMutation({ renderResult: !options.silent });
        }
        if (!serverStateKnown) {
          restoreReviewState(rollbackSnapshot);
          app.saveUncertain = true;
          if (operation.kind === "merge_coverage") {
            deferCoverageOperation(operation);
          }
        } else if (operation.kind === "merge_coverage" && !coverageOperationPresent(operation)) {
          deferCoverageOperation(operation);
        }
        if (!options.silent) {
          showErrors([{ message: error instanceof Error ? error.message : "The change could not be saved." }]);
          announceSave("Save failed. Review the error summary.");
        } else {
          setConnectionStatus("Coverage save deferred");
        }
        return false;
      } finally {
        app.pendingMutations = Math.max(0, app.pendingMutations - 1);
        if (app.saveUncertain) {
          renderApp();
        } else if (!options.silent) {
          renderApp();
        } else if (app.state !== stateBeforeRequest || isFinalizationRecovery()) {
          if (app.state === "ready" && (app.decisionDirty || app.splitDirty)) {
            renderReadyStateWithoutDiscardingForm();
          } else {
            renderApp();
          }
        } else {
          renderCoordinates();
          updateMutationControls();
        }
      }
    };

    const queued = app.mutationTail.then(job, job);
    app.mutationTail = queued.then(() => undefined, () => undefined);
    return queued;
  }

  function renderReadyStateWithoutDiscardingForm() {
    app.reviewingReady = true;
    setStateBadge();
    renderProgress();
    renderRecordingList();
    renderCoordinates();
    elements["continue-attestation"].hidden = false;
    updateMutationControls();
    setConnectionStatus("Coverage saved; draft ready. Unsaved choices remain in the form.");
  }

  function snapshotReviewState() {
    return {
      state: app.state,
      revision: app.revision,
      csrfToken: app.csrfToken,
      recordings: structuredCloneSafe(app.recordings),
      progress: structuredCloneSafe(app.progress),
      completedDigest: app.completedDigest,
      recordingId: currentRecording()?.recording_id ?? null,
      decisionId: currentInterval()?.selection_decision_id ?? null,
      decisionDirty: app.decisionDirty,
      splitDirty: app.splitDirty,
      reviewingReady: app.reviewingReady,
    };
  }

  function restoreReviewState(snapshot) {
    app.state = snapshot.state;
    app.revision = snapshot.revision;
    app.csrfToken = snapshot.csrfToken;
    app.recordings = snapshot.recordings;
    app.progress = snapshot.progress;
    app.completedDigest = snapshot.completedDigest;
    app.decisionDirty = snapshot.decisionDirty;
    app.splitDirty = snapshot.splitDirty;
    app.reviewingReady = snapshot.reviewingReady;
    const recordingIndex = app.recordings.findIndex((row) => row.recording_id === snapshot.recordingId);
    app.recordingIndex = recordingIndex >= 0 ? recordingIndex : 0;
    const intervals = currentRecording()?.intervals ?? [];
    const intervalIndex = intervals.findIndex((row) => row.selection_decision_id === snapshot.decisionId);
    app.intervalIndex = intervalIndex >= 0 ? intervalIndex : 0;
  }

  function coverageOperationPresent(operation) {
    const interval = findInterval(operation.selection_decision_id);
    if (!interval || !Array.isArray(interval.playback_coverage_ranges)) {
      return false;
    }
    return interval.playback_coverage_ranges.some(
      (range) => Number(range?.start_ms) <= operation.start_ms && Number(range?.end_ms) >= operation.end_ms,
    );
  }

  function deferCoverageOperation(operation) {
    if (coverageOperationPresent(operation)) {
      return;
    }
    const existing = app.deferredCoverage.find(
      (item) => item.selection_decision_id === operation.selection_decision_id
        && operation.start_ms <= item.end_ms + 250
        && operation.end_ms >= item.start_ms - 250,
    );
    if (existing) {
      existing.start_ms = Math.min(existing.start_ms, operation.start_ms);
      existing.end_ms = Math.max(existing.end_ms, operation.end_ms);
    } else {
      app.deferredCoverage.push({ ...operation });
    }
  }

  function preserveCoverageDraft() {
    const draft = app.coverageDraft;
    if (draft && draft.endMs - draft.startMs >= COVERAGE_MINIMUM_MS) {
      deferCoverageOperation({
        kind: "merge_coverage",
        selection_decision_id: draft.selectionDecisionId,
        start_ms: draft.startMs,
        end_ms: draft.endMs,
      });
    }
    app.coverageDraft = null;
  }

  async function refreshAfterConflict({ renderResult = true } = {}) {
    setConnectionStatus("Refreshing stale revision…");
    await loadBootstrap({ showLoading: false, preserveSelection: true, renderResult });
  }

  async function reconcileRejectedMutation({ renderResult = true } = {}) {
    try {
      await loadBootstrap({ showLoading: false, preserveSelection: true, renderResult });
      return true;
    } catch {
      setConnectionStatus("Server state needs reload");
      return false;
    }
  }

  function playNamedSegment(kind) {
    const interval = currentInterval();
    const recording = currentRecording();
    if (!interval || !recording || app.loadedMediaUrl === null) {
      announceMedia("Verified parent media is not available for playback.", true);
      return;
    }
    let startMs = interval.proposal_start_ms;
    let endMs = interval.proposal_end_ms;
    let label = "proposal";
    if (kind === "context") {
      startMs = Math.max(0, interval.proposal_start_ms - CONTEXT_PADDING_MS);
      const mediaDuration = effectiveMediaDurationMs(recording);
      endMs = Math.min(mediaDuration, interval.proposal_end_ms + CONTEXT_PADDING_MS);
      label = "context";
    } else if (
      kind === "accepted"
      && interval.decision === "include"
      && Number.isInteger(interval.accepted_start_ms)
      && Number.isInteger(interval.accepted_end_ms)
    ) {
      startMs = interval.accepted_start_ms;
      endMs = interval.accepted_end_ms;
      label = "accepted range";
    } else if (kind === "accepted") {
      announceMedia("Save an included range before playing the accepted segment.", true);
      return;
    }
    playSegment(startMs, endMs, label);
  }

  async function playSegment(startMs, endMs, label) {
    const video = elements["parent-video"];
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      announceMedia("The requested playback segment is invalid.", true);
      return;
    }
    try {
      await mediaReady(video);
      flushCoverage();
      app.activeSegment = { startMs, endMs, label };
      video.currentTime = startMs / 1000;
      app.lastPlaybackMs = startMs;
      await video.play();
      announceMedia(`Playing ${label}, ${formatCoordinate(startMs)} through ${formatCoordinate(endMs)}.`);
    } catch {
      announceMedia("Playback could not start. Use the native video controls to retry.", true);
    }
  }

  function mediaReady(video) {
    if (video.readyState >= HTMLMediaElement.HAVE_METADATA) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        video.removeEventListener("loadedmetadata", onReady);
        video.removeEventListener("error", onError);
      };
      const onReady = () => {
        cleanup();
        resolve();
      };
      const onError = () => {
        cleanup();
        reject(new Error("media unavailable"));
      };
      video.addEventListener("loadedmetadata", onReady, { once: true });
      video.addEventListener("error", onError, { once: true });
    });
  }

  function handleMediaMetadata() {
    const recording = currentRecording();
    const videoDurationMs = Number.isFinite(elements["parent-video"].duration)
      ? Math.round(elements["parent-video"].duration * 1000)
      : null;
    const expectedDurationMs = Number(recording?.media?.duration_ms);
    if (
      videoDurationMs !== null
      && Number.isFinite(expectedDurationMs)
      && Math.abs(videoDurationMs - expectedDurationMs) > 1_500
    ) {
      announceMedia("Media loaded, but its browser-reported duration differs from the verified server metadata.", true);
      return;
    }
    announceMedia(`Direct parent media ready${videoDurationMs === null ? "" : `, ${formatDuration(videoDurationMs)}`}.`);
  }

  function handlePlaybackProgress() {
    capturePlaybackSample();
    const video = elements["parent-video"];
    if (!app.activeSegment || video.seeking) {
      return;
    }
    const currentMs = safeCurrentMediaMs();
    if (currentMs + 40 < app.activeSegment.endMs) {
      return;
    }
    flushCoverage();
    if (elements["loop-segment"].checked) {
      video.currentTime = app.activeSegment.startMs / 1000;
      app.lastPlaybackMs = app.activeSegment.startMs;
      video.play().catch(() => announceMedia("Segment loop could not continue.", true));
    } else {
      video.pause();
      video.currentTime = app.activeSegment.endMs / 1000;
      app.lastPlaybackMs = null;
      announceMedia(`${titleCase(app.activeSegment.label)} playback complete.`);
    }
  }

  function capturePlaybackSample(force = false) {
    const video = elements["parent-video"];
    const interval = currentInterval();
    if (!interval || video.seeking || (!force && (video.paused || video.ended))) {
      app.lastPlaybackMs = safeCurrentMediaMs();
      return;
    }
    const currentMs = safeCurrentMediaMs();
    const previousMs = app.lastPlaybackMs;
    app.lastPlaybackMs = currentMs;
    if (
      previousMs === null
      || currentMs <= previousMs
      || currentMs - previousMs > COVERAGE_MAX_SAMPLE_GAP_MS
    ) {
      return;
    }
    const startMs = Math.max(interval.proposal_start_ms, Math.floor(previousMs));
    const endMs = Math.min(interval.proposal_end_ms, Math.ceil(currentMs));
    if (endMs - startMs < 1) {
      return;
    }
    mergeCoverageDraft(interval.selection_decision_id, startMs, endMs);
  }

  function mergeCoverageDraft(decisionId, startMs, endMs) {
    const draft = app.coverageDraft;
    if (
      draft
      && draft.selectionDecisionId === decisionId
      && startMs <= draft.endMs + 250
    ) {
      draft.startMs = Math.min(draft.startMs, startMs);
      draft.endMs = Math.max(draft.endMs, endMs);
      return;
    }
    flushCoverage();
    app.coverageDraft = { selectionDecisionId: decisionId, startMs, endMs };
  }

  function flushCoverage() {
    if (app.saveUncertain) {
      return;
    }
    const draft = app.coverageDraft;
    app.coverageDraft = null;
    const operations = app.deferredCoverage.splice(0);
    if (draft && draft.endMs - draft.startMs >= COVERAGE_MINIMUM_MS) {
      operations.push({
        kind: "merge_coverage",
        selection_decision_id: draft.selectionDecisionId,
        start_ms: draft.startMs,
        end_ms: draft.endMs,
      });
    }
    for (const operation of operations) {
      enqueueMutation(operation, { silent: true });
    }
  }

  function pauseAndFlushPlayback() {
    capturePlaybackSample(true);
    flushCoverage();
    app.lastPlaybackMs = null;
    elements["parent-video"].pause();
  }

  function seekBy(seconds) {
    const video = elements["parent-video"];
    if (app.loadedMediaUrl === null || !Number.isFinite(video.currentTime)) {
      return;
    }
    flushCoverage();
    const duration = Number.isFinite(video.duration) ? video.duration : Number.MAX_SAFE_INTEGER;
    video.currentTime = Math.max(0, Math.min(duration, video.currentTime + seconds));
    app.lastPlaybackMs = Math.round(video.currentTime * 1000);
    announceMedia(`Moved to ${formatMediaClock(app.lastPlaybackMs)}.`);
  }

  function safeCurrentMediaMs() {
    const currentTime = elements["parent-video"].currentTime;
    return Number.isFinite(currentTime) ? Math.max(0, Math.round(currentTime * 1000)) : 0;
  }

  function effectiveMediaDurationMs(recording) {
    const browserDuration = elements["parent-video"].duration;
    if (Number.isFinite(browserDuration) && browserDuration > 0) {
      return Math.round(browserDuration * 1000);
    }
    const serverDuration = Number(recording.media?.duration_ms);
    return Number.isFinite(serverDuration) && serverDuration > 0
      ? serverDuration
      : Number.MAX_SAFE_INTEGER;
  }

  function handleKeyboardShortcut(event) {
    if (
      event.defaultPrevented
      || event.ctrlKey
      || event.metaKey
      || event.altKey
      || !event.shiftKey
      || isInteractiveTarget(event.target)
      || elements["review-workspace"].hidden
    ) {
      return;
    }

    if (event.key === " ") {
      event.preventDefault();
      const video = elements["parent-video"];
      if (video.paused) {
        video.play().catch(() => announceMedia("Playback could not start.", true));
      } else {
        video.pause();
      }
    } else if (event.key.toLowerCase() === "j") {
      event.preventDefault();
      seekBy(-5);
    } else if (event.key.toLowerCase() === "k") {
      event.preventDefault();
      seekBy(5);
    } else if (event.code === "BracketLeft") {
      event.preventDefault();
      selectRelativeInterval(-1);
    } else if (event.code === "BracketRight") {
      event.preventDefault();
      selectRelativeInterval(1);
    } else if (event.key.toLowerCase() === "l") {
      event.preventDefault();
      elements["loop-segment"].checked = !elements["loop-segment"].checked;
      elements["loop-segment"].dispatchEvent(new Event("change"));
    }
  }

  function isInteractiveTarget(target) {
    if (!(target instanceof Element)) {
      return false;
    }
    return Boolean(
      target.closest("input, select, textarea, button, a, video, summary, [contenteditable='true'], [role='textbox']"),
    );
  }

  function presentAttestation() {
    if (app.saveUncertain || app.state !== "ready") {
      showErrors([{ message: "The server has not marked this draft ready for attestation." }]);
      return;
    }
    if (!canDiscardUnsavedChoices()) {
      return;
    }
    flushCoverage();
    app.reviewingReady = false;
    resetAttestationForm();
    app.attestationPresented = true;
    renderApp();
    elements["reviewer-id"].focus();
  }

  function resetAttestationForm() {
    elements["attestation-form"].reset();
    for (const id of ["attest-parent-media", "attest-no-asr", "attest-no-reference", "attest-selection-basis"]) {
      elements[id].checked = false;
    }
    elements["reviewer-id"].value = "";
    elements["attestation-save-status"].textContent = "";
  }

  function configureAttestationScreen(recovery) {
    elements["attestation-title"].textContent = recovery
      ? "Resume private review finalization"
      : "Attest and complete this private review";
    elements["attestation-intro"].textContent = recovery
      ? "A prior attestation intent was durably recorded, but finalization did not finish. Editing is disabled. Re-enter the same reviewer ID and explicitly reconfirm all four statements to resume safely."
      : "The draft is ready, but it is not valid or completed until all four statements are explicitly confirmed and the server accepts finalization.";
    elements["return-to-review"].hidden = recovery;
    elements["finalize-review"].textContent = recovery
      ? "Resume private finalization"
      : "Complete private review";
  }

  function renderAttestationSummary() {
    const metrics = deriveProgress();
    const summary = elements["attestation-progress"];
    summary.replaceChildren();
    const values = [
      `${metrics.decidedCount} intervals decided`,
      `${formatDuration(metrics.acceptedDurationMs)} accepted`,
      `${app.recordings.length} recordings`,
    ];
    for (const value of values) {
      const badge = document.createElement("span");
      badge.textContent = value;
      summary.append(badge);
    }
  }

  async function finalizeReview(event) {
    event.preventDefault();
    clearErrors();
    if (app.saveUncertain || (app.state !== "ready" && !isFinalizationRecovery())) {
      showErrors([{ message: "The server has not marked this draft ready for finalization." }]);
      return;
    }

    const errors = validateAttestation();
    if (errors.length > 0) {
      showErrors(errors);
      return;
    }

    elements["finalize-review"].disabled = true;
    elements["attestation-form"].setAttribute("aria-busy", "true");
    elements["attestation-save-status"].textContent = "Waiting for pending saves…";
    await app.mutationTail;
    if (app.saveUncertain || (app.state !== "ready" && !isFinalizationRecovery())) {
      elements["finalize-review"].disabled = false;
      renderApp();
      return;
    }

    elements["attestation-save-status"].textContent = "Submitting explicit attestations…";
    let serverStateKnown = false;
    try {
      const response = await fetch(API.finalize, {
        method: "POST",
        credentials: "same-origin",
        cache: "no-store",
        redirect: "error",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-HIMR-CSRF": app.csrfToken,
        },
        body: JSON.stringify({
          expected_revision: app.revision,
          reviewer_id: elements["reviewer-id"].value.trim(),
          direct_parent_media_reviewed: true,
          asr_outputs_inspected: false,
          reference_text_inspected: false,
          selection_basis: "source_metadata_and_direct_parent_media_only",
        }),
      });
      const payload = await readJson(response);
      if (response.status === 409 || payload?.error === "stale_revision") {
        await refreshAfterConflict();
        serverStateKnown = true;
        resetAttestationForm();
        throw new Error("The draft revision changed before finalization. The latest revision was loaded; inspect it before attesting again.");
      }
      if (!response.ok) {
        throw new Error(safeServerMessage(payload, "The server rejected finalization."));
      }
      if (looksLikeBootstrap(payload)) {
        adoptBootstrap(payload, {});
        serverStateKnown = true;
      } else {
        await loadBootstrap({ showLoading: false, preserveSelection: false });
        serverStateKnown = true;
      }
      if (app.state !== "completed") {
        throw new Error("The server did not confirm a completed review state.");
      }
      clearErrors();
      setConnectionStatus("Completed revision loaded");
      renderApp();
      elements["completed-panel"].focus?.();
    } catch (error) {
      if (!serverStateKnown) {
        try {
          await loadBootstrap({ showLoading: false, preserveSelection: false });
          serverStateKnown = true;
        } catch {
          app.saveUncertain = true;
        }
      }
      if (serverStateKnown && app.state === "completed") {
        clearErrors();
        setConnectionStatus("Completed revision loaded after connection recovery");
        renderApp();
        elements["completed-panel"].focus?.();
      } else {
        resetAttestationForm();
        renderApp();
        elements["attestation-save-status"].textContent = "Finalization was not completed. Explicit re-attestation is required before retrying.";
        showErrors([{ message: error instanceof Error ? error.message : "Finalization failed and was not completed." }]);
      }
    } finally {
      elements["finalize-review"].disabled = false;
      elements["attestation-form"].setAttribute("aria-busy", "false");
    }
  }

  function validateAttestation() {
    const errors = [];
    const reviewerId = elements["reviewer-id"].value.trim();
    if (!REVIEWER_ID_PATTERN.test(reviewerId)) {
      errors.push({ id: "reviewer-id", message: "Enter a valid assigned reviewer ID (3–128 allowed characters, beginning with a letter)." });
    } else if (SHA256_PATTERN.test(reviewerId) || SYSTEM_ID_PATTERN.test(reviewerId)) {
      errors.push({ id: "reviewer-id", message: "Enter your assigned reviewer ID, not a manifest digest or system object identifier." });
    }
    const required = [
      ["attest-parent-media", "Confirm that you reviewed the direct parent media."],
      ["attest-no-asr", "Confirm that you did not inspect ASR output."],
      ["attest-no-reference", "Confirm that you did not inspect reference text."],
      ["attest-selection-basis", "Confirm the permitted source-metadata and direct-parent-media selection basis."],
    ];
    for (const [id, message] of required) {
      if (!elements[id].checked) {
        errors.push({ id, message });
      }
    }
    return errors;
  }

  function showErrors(errors, focus = true) {
    clearFieldErrors();
    const list = elements["error-list"];
    list.replaceChildren();
    for (const error of errors) {
      const item = document.createElement("li");
      if (error.id && document.getElementById(error.id)) {
        const link = document.createElement("a");
        link.href = `#${error.id}`;
        link.textContent = error.message;
        link.addEventListener("click", (event) => {
          event.preventDefault();
          focusErrorTarget(error.id);
        });
        item.append(link);
        document.getElementById(error.id)?.setAttribute("aria-invalid", "true");
      } else {
        item.textContent = error.message;
      }
      list.append(item);
    }
    elements["error-summary"].hidden = false;
    if (focus) {
      elements["error-summary"].focus();
    }
  }

  function clearErrors() {
    elements["error-summary"].hidden = true;
    elements["error-list"].replaceChildren();
    clearFieldErrors();
  }

  function clearFieldErrors() {
    document.querySelectorAll("[aria-invalid='true']").forEach((element) => {
      element.removeAttribute("aria-invalid");
    });
  }

  function focusErrorTarget(id) {
    const target = document.getElementById(id);
    if (!(target instanceof HTMLElement)) {
      return;
    }
    target.scrollIntoView({ block: "center" });
    if (target.matches("fieldset")) {
      target.querySelector("input, select, button")?.focus();
    } else {
      target.focus();
    }
  }

  function failClosed(message) {
    app.state = "invalid";
    setStateBadge();
    setConnectionStatus("Load failed");
    showOnlyPanel("invalid-panel");
    showErrors([{ message }]);
  }

  function setConnectionStatus(message) {
    elements["connection-status"].textContent = message;
  }

  function announceSave(message) {
    elements["save-indicator"].textContent = message;
  }

  function announceMedia(message, isError = false) {
    elements["media-status"].textContent = message;
    elements["media-status"].classList.toggle("text-danger", isError);
  }

  function currentRecording() {
    return app.recordings[app.recordingIndex] ?? null;
  }

  function currentInterval() {
    return currentRecording()?.intervals?.[app.intervalIndex] ?? null;
  }

  function findRecording(recordingId) {
    return app.recordings.find((recording) => recording.recording_id === recordingId) ?? null;
  }

  function findInterval(decisionId) {
    for (const recording of app.recordings) {
      const interval = recording.intervals.find((value) => value.selection_decision_id === decisionId);
      if (interval) return interval;
    }
    return null;
  }

  function setRadioValue(name, value) {
    document.querySelectorAll(`input[type="radio"][name="${name}"]`).forEach((input) => {
      input.checked = input.value === value;
    });
  }

  function radioValue(name) {
    return document.querySelector(`input[type="radio"][name="${name}"]:checked`)?.value ?? null;
  }

  function strictInteger(value) {
    if (!/^(?:0|[1-9][0-9]*)$/.test(value.trim())) {
      return null;
    }
    const result = Number(value);
    return Number.isSafeInteger(result) ? result : null;
  }

  function integerString(value) {
    return Number.isInteger(value) ? String(value) : "";
  }

  function displayRecordingTitle(recording, index) {
    return typeof recording.title === "string" && recording.title.trim()
      ? recording.title.trim()
      : `Recording ${index + 1}`;
  }

  function decisionLabel(interval) {
    if (interval.decision === "include") return "Included";
    if (interval.decision === "exclude") return "Excluded";
    return "Not reviewed";
  }

  function intervalStatusLabel(interval) {
    const decision = decisionLabel(interval);
    if (typeof interval.status !== "string" || !interval.status.trim()) {
      return decision;
    }
    const status = titleCase(interval.status.trim());
    return status.toLowerCase() === decision.toLowerCase() ? decision : `${decision} · ${status}`;
  }

  function intervalCoverageComplete(interval) {
    const duration = interval.proposal_end_ms - interval.proposal_start_ms;
    const coverage = Number(interval.playback_coverage_ms);
    return Number.isFinite(coverage) && coverage >= Math.max(1, duration - 1_000);
  }

  function currentMediaUsable() {
    return currentRecording()?.media?.verified === true && app.loadedMediaUrl !== null;
  }

  function isFinalizationRecovery() {
    return (app.state === "editing" || app.state === "ready")
      && app.progress?.ready_to_materialize === true;
  }

  function formatCoordinate(ms) {
    return `${Math.round(ms).toLocaleString("en-US")} ms (${formatMediaClock(ms)})`;
  }

  function formatDuration(ms) {
    if (!Number.isFinite(ms) || ms < 0) return "—";
    const rounded = Math.round(ms);
    const hours = Math.floor(rounded / 3_600_000);
    const minutes = Math.floor((rounded % 3_600_000) / 60_000);
    const seconds = Math.floor((rounded % 60_000) / 1_000);
    const milliseconds = rounded % 1_000;
    const prefix = hours > 0 ? `${hours}:${String(minutes).padStart(2, "0")}` : `${minutes}`;
    return `${prefix}:${String(seconds).padStart(2, "0")}.${String(milliseconds).padStart(3, "0")}`;
  }

  function formatMediaClock(ms) {
    if (!Number.isFinite(ms) || ms < 0) return "0:00.000";
    const rounded = Math.round(ms);
    const hours = Math.floor(rounded / 3_600_000);
    const minutes = Math.floor((rounded % 3_600_000) / 60_000);
    const seconds = Math.floor((rounded % 60_000) / 1_000);
    const milliseconds = rounded % 1_000;
    const hourPrefix = hours > 0 ? `${hours}:${String(minutes).padStart(2, "0")}` : String(minutes);
    return `${hourPrefix}:${String(seconds).padStart(2, "0")}.${String(milliseconds).padStart(3, "0")}`;
  }

  function formatBytes(bytes) {
    if (!Number.isFinite(bytes) || bytes < 0) return "";
    if (bytes < 1024) return `${Math.round(bytes)} B`;
    const units = ["KiB", "MiB", "GiB", "TiB"];
    let value = bytes / 1024;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }
    return `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unitIndex]}`;
  }

  function titleCase(value) {
    return String(value).replaceAll("_", " ").replace(/^./, (first) => first.toUpperCase());
  }

  function validDigest(value) {
    return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
  }

  function structuredCloneSafe(value) {
    if (typeof structuredClone === "function") {
      return structuredClone(value);
    }
    return JSON.parse(JSON.stringify(value));
  }

  function isPlainObject(value) {
    return Boolean(value) && typeof value === "object" && !Array.isArray(value);
  }

  function looksLikeBootstrap(value) {
    return isPlainObject(value) && STATES.has(value.state) && Array.isArray(value.recordings) && "revision" in value;
  }

  function safeServerMessage(payload, fallback) {
    if (isPlainObject(payload) && typeof payload.message === "string" && payload.message.length > 0 && payload.message.length <= 500) {
      return payload.message;
    }
    return fallback;
  }

  async function readJson(response) {
    const contentType = response.headers.get("content-type") ?? "";
    if (!contentType.toLowerCase().includes("application/json")) {
      return null;
    }
    try {
      return await response.json();
    } catch {
      return null;
    }
  }
})();
