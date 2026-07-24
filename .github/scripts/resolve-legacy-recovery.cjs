'use strict';

const CARRY_FORWARD_ARTIFACT = "legacy-recovery-carry-forward";
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

async function findCarryForward({github, owner, repo, defaultBranch, currentRunId}) {
  const candidates = [];
  for (const workflowId of ["daily.yml", "governance-cutover.yml"]) {
    const response = await github.rest.actions.listWorkflowRuns({
      owner,
      repo,
      workflow_id: workflowId,
      branch: defaultBranch,
      per_page: 100,
    });
    for (const run of response.data.workflow_runs || []) {
      if (
        String(run.id) !== currentRunId &&
        run.conclusion === "success" &&
        run.head_branch === defaultBranch &&
        CARRY_FORWARD_WORKFLOWS.has(workflowPath(run))
      ) {
        candidates.push(run);
      }
    }
  }
  candidates.sort((left, right) => {
    const byTime = String(right.created_at).localeCompare(String(left.created_at));
    return byTime || Number(right.id) - Number(left.id);
  });
  for (const run of candidates) {
    const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {
      owner,
      repo,
      run_id: run.id,
      per_page: 100,
    });
    const matches = artifacts.filter(
      (item) => item.name === CARRY_FORWARD_ARTIFACT && !item.expired,
    );
    if (matches.length > 1) {
      throw new Error(`carry-forward artifact is ambiguous in run ${run.id}`);
    }
    if (matches.length === 1) {
      const artifact = matches[0];
      if (!/^sha256:[0-9a-f]{64}$/i.test(artifact.digest || "")) {
        throw new Error("carry-forward artifact has no immutable SHA-256 digest");
      }
      return {run, artifact};
    }
  }
  return null;
}

async function resolveOriginal({github, owner, repo, defaultBranch, pin}) {
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
  const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {
    owner,
    repo,
    run_id: numericRunId,
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
    const carry = await findCarryForward({
      github,
      owner,
      repo,
      defaultBranch,
      currentRunId,
    });
    const selected = carry || (await resolveOriginal({
      github,
      owner,
      repo,
      defaultBranch,
      pin,
    }));
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

module.exports.CARRY_FORWARD_ARTIFACT = CARRY_FORWARD_ARTIFACT;
module.exports.CARRY_FORWARD_WORKFLOWS = CARRY_FORWARD_WORKFLOWS;
