"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const resolve = require("../../.github/scripts/resolve-legacy-recovery.cjs");

const PIN = {
  LEGACY_RUN_ID: "12345",
  LEGACY_ARTIFACT_NAME: "legacy-pages-archive-seed",
  LEGACY_CODE_REVISION: "a".repeat(40),
  LEGACY_ARTIFACT_DIGEST: `sha256:${"b".repeat(64)}`,
  DEFAULT_BRANCH: "main",
};

async function withEnvironment(callback) {
  const before = {};
  for (const [key, value] of Object.entries(PIN)) {
    before[key] = process.env[key];
    process.env[key] = value;
  }
  try {
    await callback();
  } finally {
    for (const [key, value] of Object.entries(before)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

function outputCollector() {
  const outputs = new Map();
  let failure = null;
  return {
    core: {
      setOutput(name, value) {
        outputs.set(name, value);
      },
      setFailed(message) {
        failure = message;
      },
    },
    outputs,
    failure: () => failure,
  };
}

test("prefers an unexpired carry-forward from a successful trusted default-branch run", async () => {
  await withEnvironment(async () => {
    const collector = outputCollector();
    const carryRun = {
      id: 777,
      conclusion: "success",
      head_branch: "main",
      head_sha: "c".repeat(40),
      path: ".github/workflows/daily.yml",
      created_at: "2026-07-22T00:00:00Z",
    };
    const github = {
      rest: {actions: {
        async listWorkflowRuns({workflow_id}) {
          return {data: {workflow_runs: workflow_id === "daily.yml" ? [carryRun] : []}};
        },
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun() {
          throw new Error("seed fallback must not be used");
        },
      }},
      async paginate(_method, args) {
        assert.equal(args.run_id, 777);
        return [{
          id: 888,
          name: "legacy-recovery-carry-forward",
          expired: false,
          digest: `sha256:${"d".repeat(64)}`,
        }];
      },
    };
    await resolve({
      github,
      context: {repo: {owner: "sung1673", repo: "activist-rss-curator"}, runId: 999},
      core: collector.core,
    });
    assert.equal(collector.failure(), null);
    assert.equal(collector.outputs.get("mode"), "carry");
    assert.equal(collector.outputs.get("artifact_id"), "888");
    assert.equal(collector.outputs.get("run_id"), "777");
  });
});

test("falls back to the exact original digest pin when no carry-forward exists", async () => {
  await withEnvironment(async () => {
    const collector = outputCollector();
    const originalRun = {
      id: 12345,
      conclusion: "success",
      head_branch: "main",
      head_sha: "a".repeat(40),
      path: ".github/workflows/build-feed.yml",
    };
    const github = {
      rest: {actions: {
        async listWorkflowRuns() {
          return {data: {workflow_runs: []}};
        },
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun() {
          return {data: originalRun};
        },
      }},
      async paginate(_method, args) {
        assert.equal(args.run_id, 12345);
        return [{
          id: 67890,
          name: "legacy-pages-archive-seed",
          expired: false,
          digest: `sha256:${"b".repeat(64)}`,
        }];
      },
    };
    await resolve({
      github,
      context: {repo: {owner: "sung1673", repo: "activist-rss-curator"}, runId: 999},
      core: collector.core,
    });
    assert.equal(collector.failure(), null);
    assert.equal(collector.outputs.get("mode"), "seed");
    assert.equal(collector.outputs.get("artifact_id"), "67890");
  });
});

test("fails closed on an ambiguous carry-forward artifact", async () => {
  await withEnvironment(async () => {
    const collector = outputCollector();
    const github = {
      rest: {actions: {
        async listWorkflowRuns({workflow_id}) {
          return {data: {workflow_runs: workflow_id === "daily.yml" ? [{
            id: 777,
            conclusion: "success",
            head_branch: "main",
            head_sha: "c".repeat(40),
            path: ".github/workflows/daily.yml",
            created_at: "2026-07-22T00:00:00Z",
          }] : []}};
        },
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun() {},
      }},
      async paginate() {
        return [1, 2].map((id) => ({
          id,
          name: "legacy-recovery-carry-forward",
          expired: false,
          digest: `sha256:${"d".repeat(64)}`,
        }));
      },
    };
    await resolve({
      github,
      context: {repo: {owner: "sung1673", repo: "activist-rss-curator"}, runId: 999},
      core: collector.core,
    });
    assert.match(collector.failure(), /ambiguous/);
    assert.equal(collector.outputs.size, 0);
  });
});
