'use strict';

const CARRY_FORWARD_ARTIFACT_PREFIX = "legacy-recovery-carry-forward-v3";
const CARRY_FORWARD_WORKFLOWS = new Set([
  ".github/workflows/daily.yml",
  ".github/workflows/governance-cutover.yml",
]);

function required(name) {
  const value = (process.env[name] || "").trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function validatePin() {
  const runId = required("LEGACY_RUN_ID");
  const artifactName = required("LEGACY_ARTIFACT_NAME");
  const codeRevision = required("LEGACY_CODE_REVISION").toLowerCase();
  const artifactDigest = required("LEGACY_ARTIFACT_DIGEST").toLowerCase();
  if (!/^[1-9][0-9]*$/.test(runId)) {
    throw new Error("LEGACY_RUN_ID must be a positive integer");
  }
  if (!/^[0-9a-f]{40}$/.test(codeRevision)) {
    throw new Error("LEGACY_CODE_REVISION must be a full Git SHA");
  }
  if (!/^sha256:[0-9a-f]{64}$/.test(artifactDigest)) {
    throw new Error("LEGACY_ARTIFACT_DIGEST must pin SHA-256");
  }
  return {runId, artifactName, codeRevision, artifactDigest};
}

function workflowPath(run) {
  return String(run.path || "").split("@")[0];
}

function carryArtifactName(pin) {
  return [
    CARRY_FORWARD_ARTIFACT_PREFIX,
    pin.runId,
    pin.artifactDigest.slice("sha256:".length),
  ].join("-");
}

function githubTimestamp(value) {
  const text = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(text)) return Number.NaN;
  const timestamp = Date.parse(text);
  return Number.isFinite(timestamp) ? timestamp : Number.NaN;
}

async function findCarryForward({
  github,
  owner,
  repo,
  defaultBranch,
  currentRunId,
  expectedArtifactName,
}) {
  const artifacts = await github.paginate(
    github.rest.actions.listArtifactsForRepo,
    {
      owner,
      repo,
      name: expectedArtifactName,
      per_page: 100,
    },
  );
  const candidates = artifacts
    .filter((artifact) => artifact.name === expectedArtifactName && !artifact.expired)
    .sort((left, right) => {
      const byTime = String(right.created_at).localeCompare(
        String(left.created_at),
      );
      return byTime || Number(right.id) - Number(left.id);
    });
  const countsByRun = new Map();
  for (const artifact of candidates) {
    const runId = artifact.workflow_run && artifact.workflow_run.id;
    if (!Number.isSafeInteger(runId) || runId <= 0) {
      throw new Error("carry-forward artifact has no valid producer run");
    }
    countsByRun.set(runId, (countsByRun.get(runId) || 0) + 1);
  }
  for (const artifact of candidates) {
    const runId = artifact.workflow_run.id;
    if (String(runId) === currentRunId) continue;
    if (countsByRun.get(runId) !== 1) {
      throw new Error(`carry-forward artifact is ambiguous in run ${runId}`);
    }
    if (
      !Number.isSafeInteger(artifact.id) ||
      artifact.id <= 0 ||
      !Number.isFinite(githubTimestamp(artifact.created_at)) ||
      !/^sha256:[0-9a-f]{64}$/i.test(artifact.digest || "")
    ) {
      throw new Error("carry-forward artifact metadata is invalid");
    }
    const response = await github.rest.actions.getWorkflowRun({
      owner,
      repo,
      run_id: runId,
    });
    const run = response.data;
    if (
      run &&
      run.id === runId &&
      run.conclusion === "success" &&
      run.head_branch === defaultBranch &&
      CARRY_FORWARD_WORKFLOWS.has(workflowPath(run)) &&
      Number.isFinite(githubTimestamp(run.created_at))
    ) {
      return {run, artifact};
    }
  }
  return null;
}

async function resolvePinnedRun({github, owner, repo, defaultBranch, pin}) {
  const numericRunId = Number(pin.runId);
  if (!Number.isSafeInteger(numericRunId)) {
    throw new Error("LEGACY_RUN_ID is outside the safe integer range");
  }
  const response = await github.rest.actions.getWorkflowRun({
    owner,
    repo,
    run_id: numericRunId,
  });
  const run = response.data;
  if (run.conclusion !== "success") throw new Error("pinned legacy run was not successful");
  if ((run.head_sha || "").toLowerCase() !== pin.codeRevision) {
    throw new Error("pinned legacy run SHA does not match the declared revision");
  }
  if (run.head_branch !== defaultBranch) {
    throw new Error("pinned legacy run is not from the default branch");
  }
  if (workflowPath(run) !== ".github/workflows/build-feed.yml") {
    throw new Error("pinned legacy run used an unexpected workflow");
  }
  if (!Number.isFinite(githubTimestamp(run.created_at))) {
    throw new Error("pinned legacy run has an invalid creation timestamp");
  }
  return run;
}

async function resolveOriginalArtifact({github, owner, repo, pin, run}) {
  const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {
    owner,
    repo,
    run_id: run.id,
    per_page: 100,
  });
  const matches = artifacts.filter(
    (item) => item.name === pin.artifactName && !item.expired,
  );
  if (matches.length !== 1) {
    throw new Error("pinned legacy artifact is missing, expired, or ambiguous");
  }
  const artifact = matches[0];
  if ((artifact.digest || "").toLowerCase() !== pin.artifactDigest) {
    throw new Error("pinned legacy artifact digest has changed");
  }
  return {run, artifact};
}

module.exports = async ({github, context, core}) => {
  try {
    const pin = validatePin();
    const owner = context.repo.owner;
    const repo = context.repo.repo;
    const defaultBranch = required("DEFAULT_BRANCH");
    const currentRunId = String(context.runId || process.env.GITHUB_RUN_ID || "");
    const expectedCarryArtifactName = carryArtifactName(pin);
    const carry = await findCarryForward({
      github,
      owner,
      repo,
      defaultBranch,
      currentRunId,
      expectedArtifactName: expectedCarryArtifactName,
    });
    let original = null;
    if (!carry) {
      const pinnedRun = await resolvePinnedRun({
        github,
        owner,
        repo,
        defaultBranch,
        pin,
      });
      original = await resolveOriginalArtifact({
        github,
        owner,
        repo,
        pin,
        run: pinnedRun,
      });
    }
    const selected = carry || original;
    core.setOutput("carry_artifact_name", expectedCarryArtifactName);
    core.setOutput("pin_run_id", pin.runId);
    core.setOutput("pin_artifact_name", pin.artifactName);
    core.setOutput("pin_code_revision", pin.codeRevision);
    core.setOutput("pin_artifact_digest", pin.artifactDigest);
    core.setOutput("mode", carry ? "carry" : "seed");
    core.setOutput("artifact_id", String(selected.artifact.id));
    core.setOutput("artifact_name", selected.artifact.name);
    core.setOutput("artifact_digest", String(selected.artifact.digest).toLowerCase());
    core.setOutput("run_id", String(selected.run.id));
    core.setOutput("producer_revision", String(selected.run.head_sha).toLowerCase());
    core.setOutput("source_workflow", workflowPath(selected.run));
  } catch (error) {
    core.setFailed(error instanceof Error ? error.message : String(error));
  }
};

module.exports.CARRY_FORWARD_ARTIFACT_PREFIX = CARRY_FORWARD_ARTIFACT_PREFIX;
module.exports.CARRY_FORWARD_WORKFLOWS = CARRY_FORWARD_WORKFLOWS;
module.exports.carryArtifactName = carryArtifactName;
