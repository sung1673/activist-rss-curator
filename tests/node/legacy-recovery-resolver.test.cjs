"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const resolve = require("../../.github/scripts/resolve-legacy-recovery.cjs");

test("uses a pin-bound v3 carry-forward namespace", () => {
  assert.equal(
    resolve.CARRY_FORWARD_ARTIFACT_PREFIX,
    "legacy-recovery-carry-forward-v3",
  );
});

const PIN = {
  LEGACY_RUN_ID: "12345",
  LEGACY_ARTIFACT_NAME: "legacy-pages-archive-seed",
  LEGACY_CODE_REVISION: "a".repeat(40),
  LEGACY_ARTIFACT_DIGEST: `sha256:${"b".repeat(64)}`,
  DEFAULT_BRANCH: "main",
};
const CARRY_NAME = `legacy-recovery-carry-forward-v3-12345-${"b".repeat(64)}`;

function carryArtifact(overrides = {}) {
  return {
    id: 888,
    name: CARRY_NAME,
    expired: false,
    digest: `sha256:${"d".repeat(64)}`,
    created_at: "2026-07-23T00:01:00Z",
    workflow_run: {id: 777},
    ...overrides,
  };
}

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
      created_at: "2026-07-23T00:00:00Z",
    };
    const github = {
      rest: {actions: {
        async listArtifactsForRepo() {},
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun({run_id}) {
          assert.equal(run_id, 777);
          return {data: carryRun};
        },
      }},
      async paginate(_method, args) {
        assert.equal(args.name, CARRY_NAME);
        return [carryArtifact()];
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
    assert.equal(collector.outputs.get("carry_artifact_name"), CARRY_NAME);
    assert.equal(collector.outputs.get("pin_run_id"), "12345");
    assert.equal(
      collector.outputs.get("pin_artifact_digest"),
      `sha256:${"b".repeat(64)}`,
    );
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
      created_at: "2026-07-22T00:00:00Z",
    };
    const github = {
      rest: {actions: {
        async listArtifactsForRepo() {},
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun() {
          return {data: originalRun};
        },
      }},
      async paginate(_method, args) {
        if (args.name === CARRY_NAME) return [];
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

test("ignores an unbound legacy carry-forward from before pin rotation", async () => {
  await withEnvironment(async () => {
    const collector = outputCollector();
    const originalRun = {
      id: 12345,
      conclusion: "success",
      head_branch: "main",
      head_sha: "a".repeat(40),
      path: ".github/workflows/build-feed.yml",
      created_at: "2026-07-31T00:00:00Z",
    };
    const github = {
      rest: {actions: {
        async listArtifactsForRepo() {},
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun() {
          return {data: originalRun};
        },
      }},
      async paginate(_method, args) {
        if (args.name === CARRY_NAME) return [];
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
      context: {
        repo: {owner: "sung1673", repo: "activist-rss-curator"},
        runId: 999,
      },
      core: collector.core,
    });
    assert.equal(collector.failure(), null);
    assert.equal(collector.outputs.get("mode"), "seed");
    assert.equal(collector.outputs.get("artifact_id"), "67890");
    assert.equal(collector.outputs.get("run_id"), "12345");
  });
});

test("uses a valid pin-bound carry when the original seed artifact has expired", async () => {
  await withEnvironment(async () => {
    const collector = outputCollector();
    const carryRun = {
      id: 777,
      conclusion: "success",
      head_branch: "main",
      head_sha: "c".repeat(40),
      path: ".github/workflows/daily.yml",
      created_at: "2026-07-23T00:00:00Z",
    };
    const github = {
      rest: {actions: {
        async listArtifactsForRepo() {},
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun({run_id}) {
          assert.equal(run_id, 777, "the original seed run must not be read");
          return {data: carryRun};
        },
      }},
      async paginate(_method, args) {
        assert.equal(args.name, CARRY_NAME);
        return [carryArtifact()];
      },
    };
    await resolve({
      github,
      context: {
        repo: {owner: "sung1673", repo: "activist-rss-curator"},
        runId: 999,
      },
      core: collector.core,
    });
    assert.equal(collector.failure(), null);
    assert.equal(collector.outputs.get("mode"), "carry");
    assert.equal(collector.outputs.get("artifact_id"), "888");
  });
});

test("ignores a newer carry-forward bound to a different pin", async () => {
  await withEnvironment(async () => {
    const collector = outputCollector();
    const github = {
      rest: {actions: {
        async listArtifactsForRepo() {},
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun() {
          return {data: {
            id: 12345,
            conclusion: "success",
            head_branch: "main",
            head_sha: "a".repeat(40),
            path: ".github/workflows/build-feed.yml",
            created_at: "2026-07-22T00:00:00Z",
          }};
        },
      }},
      async paginate(_method, args) {
        if (args.name === CARRY_NAME) return [];
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
      context: {
        repo: {owner: "sung1673", repo: "activist-rss-curator"},
        runId: 999,
      },
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
        async listArtifactsForRepo() {},
        async listWorkflowRunArtifacts() {},
        async getWorkflowRun() {
          throw new Error("ambiguous carry must fail before producer lookup");
        },
      }},
      async paginate(_method, args) {
        assert.equal(args.name, CARRY_NAME);
        return [
          carryArtifact({id: 1}),
          carryArtifact({id: 2, created_at: "2026-07-23T00:00:59Z"}),
        ];
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
