"use strict";

const BUILD_FEED_WORKFLOW = ".github/workflows/build-feed.yml";
const MINIMUM_AGE_MS = 30 * 60 * 1000;

function statusOf(error) {
  const direct = Number(error && error.status);
  if (Number.isInteger(direct)) return direct;
  const response = Number(error && error.response && error.response.status);
  return Number.isInteger(response) ? response : 0;
}

function isServerError(error) {
  const status = statusOf(error);
  return status >= 500 && status <= 599;
}

function countIsExactlyZero(response, collectionKey) {
  const data = response && response.data;
  return (
    data &&
    Number.isInteger(data.total_count) &&
    data.total_count === 0 &&
    Array.isArray(data[collectionKey]) &&
    data[collectionKey].length === 0
  );
}

function normalizedSha(value) {
  const sha = String(value || "").trim().toLowerCase();
  return /^[0-9a-f]{40}$/.test(sha) ? sha : "";
}

function parsedGitHubTimestamp(value) {
  const timestamp = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(timestamp)) {
    return Number.NaN;
  }
  const timestampMs = Date.parse(timestamp);
  if (
    !Number.isFinite(timestampMs) ||
    new Date(timestampMs).toISOString().replace(".000Z", "Z") !== timestamp
  ) {
    return Number.NaN;
  }
  return timestampMs;
}

function isSameImmutableRunSnapshot(listedRun, liveRun) {
  const immutableFields = [
    "id",
    "path",
    "status",
    "head_sha",
    "run_attempt",
    "created_at",
    "run_started_at",
    "updated_at",
  ];
  return immutableFields.every(
    (field) => listedRun && liveRun && listedRun[field] === liveRun[field],
  );
}

function qualifyOrphanedUnstarted({
  listedRun,
  liveRun,
  workflowId,
  currentSha,
  nowMs,
  workflow,
  jobs,
  artifacts,
}) {
  const runId = liveRun && liveRun.id;
  const runSha = normalizedSha(liveRun && liveRun.head_sha);
  const expectedSha = normalizedSha(currentSha);
  const createdAt = String((liveRun && liveRun.created_at) || "").trim();
  const createdAtMs = parsedGitHubTimestamp(createdAt);

  if (
    workflowId !== BUILD_FEED_WORKFLOW ||
    !Number.isSafeInteger(runId) ||
    runId <= 0 ||
    !isSameImmutableRunSnapshot(listedRun, liveRun) ||
    liveRun.path !== BUILD_FEED_WORKFLOW ||
    liveRun.status !== "queued" ||
    liveRun.run_attempt !== 1 ||
    liveRun.run_started_at !== createdAt ||
    liveRun.updated_at !== createdAt ||
    !runSha ||
    !expectedSha ||
    runSha === expectedSha ||
    !Number.isSafeInteger(nowMs) ||
    !Number.isFinite(createdAtMs) ||
    nowMs - createdAtMs < MINIMUM_AGE_MS ||
    !workflow ||
    !workflow.data ||
    workflow.data.path !== BUILD_FEED_WORKFLOW ||
    workflow.data.state !== "disabled_manually" ||
    !countIsExactlyZero(jobs, "jobs") ||
    !countIsExactlyZero(artifacts, "artifacts")
  ) {
    return null;
  }

  return {
    run_id: runId,
    head_sha: runSha,
    created_at: createdAt,
  };
}

function classifyOrphanedUnstarted({
  cancelError,
  forceError,
  ...candidate
}) {
  if (!isServerError(cancelError) || !isServerError(forceError)) {
    return null;
  }
  return qualifyOrphanedUnstarted(candidate);
}

async function confirmTerminalAfterCancelConflict({
  github,
  owner,
  repo,
  runId,
  error,
}) {
  if (
    ![409, 422].includes(statusOf(error)) ||
    !Number.isSafeInteger(runId) ||
    runId <= 0
  ) {
    throw error;
  }
  const response = await github.rest.actions.getWorkflowRun({
    owner,
    repo,
    run_id: runId,
  });
  if (
    !response ||
    !response.data ||
    response.data.id !== runId ||
    response.data.status !== "completed"
  ) {
    throw error;
  }
}

async function readCandidateEvidence({github, owner, repo, run}) {
  const liveRunResponse = await github.rest.actions.getWorkflowRun({
    owner,
    repo,
    run_id: run.id,
  });
  const workflow = await github.rest.actions.getWorkflow({
    owner,
    repo,
    workflow_id: BUILD_FEED_WORKFLOW,
  });
  const jobs = await github.rest.actions.listJobsForWorkflowRun({
    owner,
    repo,
    run_id: run.id,
    filter: "all",
    per_page: 1,
  });
  const artifacts = await github.rest.actions.listWorkflowRunArtifacts({
    owner,
    repo,
    run_id: run.id,
    per_page: 1,
  });
  return {
    liveRun: liveRunResponse && liveRunResponse.data,
    workflow,
    jobs,
    artifacts,
  };
}

async function revalidateOrphanedUnstarted({
  github,
  owner,
  repo,
  listedRun,
  currentSha,
  nowMs,
}) {
  if (
    !listedRun ||
    !Number.isSafeInteger(listedRun.id) ||
    listedRun.id <= 0
  ) {
    throw new Error("Orphaned Pages run revalidation requires a valid run");
  }
  const current = await readCandidateEvidence({
    github,
    owner,
    repo,
    run: listedRun,
  });
  const revalidated = qualifyOrphanedUnstarted({
    listedRun,
    liveRun: current.liveRun,
    workflowId: BUILD_FEED_WORKFLOW,
    currentSha,
    nowMs,
    workflow: current.workflow,
    jobs: current.jobs,
    artifacts: current.artifacts,
  });
  if (!revalidated) {
    throw new Error(
      `Orphaned Pages run ${listedRun.id} changed after quarantine`,
    );
  }
  return revalidated;
}

async function handleCancelServerError({
  github,
  owner,
  repo,
  run,
  workflowId,
  currentSha,
  nowMs,
  cancelError,
}) {
  if (
    !isServerError(cancelError) ||
    !run ||
    !Number.isSafeInteger(run.id) ||
    run.id <= 0
  ) {
    throw cancelError;
  }

  const beforeForce = await readCandidateEvidence({github, owner, repo, run});
  const qualifiedBeforeForce = qualifyOrphanedUnstarted({
    listedRun: run,
    liveRun: beforeForce.liveRun,
    workflowId,
    currentSha,
    nowMs,
    workflow: beforeForce.workflow,
    jobs: beforeForce.jobs,
    artifacts: beforeForce.artifacts,
  });
  if (!qualifiedBeforeForce) {
    throw cancelError;
  }

  let forceError;
  try {
    await github.rest.actions.forceCancelWorkflowRun({
      owner,
      repo,
      run_id: run.id,
    });
    return {forceCancelled: true, orphanedUnstarted: null};
  } catch (error) {
    forceError = error;
    if (!isServerError(error)) throw error;
  }

  const afterForce = await readCandidateEvidence({github, owner, repo, run});
  const orphanedUnstarted = classifyOrphanedUnstarted({
    listedRun: run,
    liveRun: afterForce.liveRun,
    workflowId,
    currentSha,
    nowMs,
    workflow: afterForce.workflow,
    jobs: afterForce.jobs,
    artifacts: afterForce.artifacts,
    cancelError,
    forceError,
  });
  if (!orphanedUnstarted) {
    throw forceError;
  }

  return {
    forceCancelled: false,
    orphanedUnstarted,
  };
}

module.exports = {
  BUILD_FEED_WORKFLOW,
  MINIMUM_AGE_MS,
  classifyOrphanedUnstarted,
  confirmTerminalAfterCancelConflict,
  handleCancelServerError,
  isServerError,
  qualifyOrphanedUnstarted,
  revalidateOrphanedUnstarted,
  statusOf,
};
