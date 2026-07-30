"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const {
  BUILD_FEED_WORKFLOW,
  classifyOrphanedUnstarted,
  confirmTerminalAfterCancelConflict,
  handleCancelServerError,
  revalidateOrphanedUnstarted,
} = require("../../.github/scripts/orphaned-pages-run.cjs");

const CURRENT_SHA = "3017887115274c6ecd26b6ab2cb162e19674c0b3";
const OLD_SHA = "5bc1b84d481a3af25dd0b287a6dc69e0d95cd99d";
const CREATED_AT = "2026-07-30T10:37:27Z";
const NOW = Date.parse("2026-07-30T11:07:27Z");
const PRODUCTION_GHOST_RUN_ID = 30535379482;

function serverError(status) {
  return Object.assign(new Error(`server ${status}`), {status});
}

function fixture(overrides = {}) {
  const calls = [];
  const run = {
    id: PRODUCTION_GHOST_RUN_ID,
    path: BUILD_FEED_WORKFLOW,
    status: "queued",
    head_sha: OLD_SHA,
    run_attempt: 1,
    created_at: CREATED_AT,
    run_started_at: CREATED_AT,
    updated_at: CREATED_AT,
    ...overrides.run,
  };
  const liveRun = {
    ...run,
    ...overrides.liveRun,
  };
  const afterForceRun = {
    ...liveRun,
    ...overrides.afterForceRun,
  };
  const responses = {
    liveRun: {data: liveRun},
    liveRunAfter: {data: afterForceRun},
    workflow: {
      data: {path: BUILD_FEED_WORKFLOW, state: "disabled_manually"},
    },
    workflowAfter: null,
    jobs: {data: {total_count: 0, jobs: []}},
    jobsAfter: null,
    artifacts: {data: {total_count: 0, artifacts: []}},
    artifactsAfter: null,
    forceError: serverError(503),
    ...overrides.responses,
  };
  let runReads = 0;
  let workflowReads = 0;
  let jobReads = 0;
  let artifactReads = 0;
  const github = {
    rest: {
      actions: {
        async getWorkflowRun(input) {
          calls.push(["run", input]);
          runReads += 1;
          return runReads > 1 ? responses.liveRunAfter : responses.liveRun;
        },
        async getWorkflow(input) {
          calls.push(["workflow", input]);
          workflowReads += 1;
          return workflowReads > 1 && responses.workflowAfter
            ? responses.workflowAfter
            : responses.workflow;
        },
        async listJobsForWorkflowRun(input) {
          calls.push(["jobs", input]);
          jobReads += 1;
          return jobReads > 1 && responses.jobsAfter
            ? responses.jobsAfter
            : responses.jobs;
        },
        async listWorkflowRunArtifacts(input) {
          calls.push(["artifacts", input]);
          artifactReads += 1;
          return artifactReads > 1 && responses.artifactsAfter
            ? responses.artifactsAfter
            : responses.artifacts;
        },
        async forceCancelWorkflowRun(input) {
          calls.push(["force", input]);
          if (responses.forceError) throw responses.forceError;
          return {status: 202};
        },
      },
    },
  };
  return {calls, github, liveRun, responses, run};
}

async function evaluate(overrides = {}) {
  const {calls, github, run} = fixture(overrides);
  const cancelError = Object.hasOwn(overrides, "cancelError")
    ? overrides.cancelError
    : serverError(500);
  const result = await handleCancelServerError({
    github,
    owner: "owner",
    repo: "repo",
    run,
    workflowId: Object.hasOwn(overrides, "workflowId")
      ? overrides.workflowId
      : BUILD_FEED_WORKFLOW,
    currentSha: Object.hasOwn(overrides, "currentSha")
      ? overrides.currentSha
      : CURRENT_SHA,
    nowMs: Object.hasOwn(overrides, "nowMs") ? overrides.nowMs : NOW,
    cancelError,
  });
  return {calls, result};
}

test("quarantines only a disabled, unstarted, old queued build after both server errors", async () => {
  const {calls, result} = await evaluate();
  assert.deepEqual(result, {
    forceCancelled: false,
    orphanedUnstarted: {
      run_id: PRODUCTION_GHOST_RUN_ID,
      head_sha: OLD_SHA,
      created_at: CREATED_AT,
    },
  });
  assert.deepEqual(
    calls.map(([name]) => name),
    [
      "run",
      "workflow",
      "jobs",
      "artifacts",
      "force",
      "run",
      "workflow",
      "jobs",
      "artifacts",
    ],
  );
});

test("production ghost metadata qualifies without a SHA-level deployment lookup", () => {
  const {liveRun, responses, run} = fixture();
  assert.deepEqual(
    classifyOrphanedUnstarted({
      listedRun: run,
      liveRun,
      workflowId: BUILD_FEED_WORKFLOW,
      currentSha: CURRENT_SHA,
      nowMs: NOW,
      workflow: responses.workflow,
      jobs: responses.jobs,
      artifacts: responses.artifacts,
      cancelError: serverError(500),
      forceError: serverError(503),
    }),
    {
      run_id: PRODUCTION_GHOST_RUN_ID,
      head_sha: OLD_SHA,
      created_at: CREATED_AT,
    },
  );
});

test("production ghost metadata does not classify unless both cancellations are server errors", () => {
  const {liveRun, responses, run} = fixture();
  const candidate = {
    listedRun: run,
    liveRun,
    workflowId: BUILD_FEED_WORKFLOW,
    currentSha: CURRENT_SHA,
    nowMs: NOW,
    workflow: responses.workflow,
    jobs: responses.jobs,
    artifacts: responses.artifacts,
  };
  assert.equal(
    classifyOrphanedUnstarted({
      ...candidate,
      cancelError: serverError(422),
      forceError: serverError(503),
    }),
    null,
  );
  assert.equal(
    classifyOrphanedUnstarted({
      ...candidate,
      cancelError: serverError(500),
      forceError: serverError(409),
    }),
    null,
  );
});

test("successful force cancellation is never classified as orphaned", async () => {
  const {result} = await evaluate({responses: {forceError: null}});
  assert.deepEqual(result, {
    forceCancelled: true,
    orphanedUnstarted: null,
  });
});

for (const [name, overrides] of [
  ["daily workflow", {workflowId: ".github/workflows/daily.yml"}],
  ["current SHA", {run: {head_sha: CURRENT_SHA}}],
  ["started run", {run: {status: "in_progress"}}],
  ["second attempt", {run: {run_attempt: 2}}],
  [
    "run with a later start timestamp",
    {run: {run_started_at: "2026-07-30T10:37:28Z"}},
  ],
  [
    "run whose metadata was updated",
    {run: {updated_at: "2026-07-30T10:37:28Z"}},
  ],
  [
    "run that changed between scan and inspection",
    {liveRun: {updated_at: "2026-07-30T10:37:28Z"}},
  ],
  [
    "young run",
    {
      run: {
        created_at: "2026-07-30T10:37:28Z",
        run_started_at: "2026-07-30T10:37:28Z",
        updated_at: "2026-07-30T10:37:28Z",
      },
    },
  ],
  [
    "enabled workflow",
    {
      responses: {
        workflow: {
          data: {path: BUILD_FEED_WORKFLOW, state: "active"},
        },
      },
    },
  ],
  [
    "run with a job",
    {
      responses: {
        jobs: {data: {total_count: 1, jobs: [{id: 1}]}},
      },
    },
  ],
  [
    "run with an artifact",
    {
      responses: {
        artifacts: {
          data: {total_count: 1, artifacts: [{id: 1}]},
        },
      },
    },
  ],
  ["invalid current SHA", {currentSha: "not-a-sha"}],
  ["invalid current time", {nowMs: Number.NaN}],
]) {
  test(`${name} remains fail-closed`, async () => {
    await assert.rejects(() => evaluate(overrides), /server 500/);
  });
}

test("non-server cancel and force errors remain fail-closed", async () => {
  await assert.rejects(
    () => evaluate({cancelError: serverError(422)}),
    /server 422/,
  );
  await assert.rejects(
    () => evaluate({responses: {forceError: serverError(409)}}),
    /server 409/,
  );
});

test("malformed zero-count API responses remain fail-closed", async () => {
  for (const responses of [
    {jobs: {data: {total_count: 0}}},
    {jobs: {data: {total_count: 1, jobs: []}}},
    {artifacts: {data: {total_count: 0}}},
    {artifacts: {data: {total_count: 1, artifacts: []}}},
  ]) {
    await assert.rejects(() => evaluate({responses}), /server 500/);
  }
});

test("a run that changes after force-cancel remains fail-closed", async () => {
  await assert.rejects(
    () =>
      evaluate({
        afterForceRun: {updated_at: "2026-07-30T10:37:28Z"},
      }),
    /server 503/,
  );
  await assert.rejects(
    () =>
      evaluate({
        responses: {
          jobsAfter: {data: {total_count: 1, jobs: [{id: 7}]}},
        },
      }),
    /server 503/,
  );
});

test("quarantined runs are fail-closed if they start, gain jobs, or their workflow activates", async () => {
  const classified = fixture();
  const audit = classifyOrphanedUnstarted({
    listedRun: classified.run,
    liveRun: classified.liveRun,
    workflowId: BUILD_FEED_WORKFLOW,
    currentSha: CURRENT_SHA,
    nowMs: NOW,
    workflow: classified.responses.workflow,
    jobs: classified.responses.jobs,
    artifacts: classified.responses.artifacts,
    cancelError: serverError(500),
    forceError: serverError(503),
  });
  assert.ok(audit);

  const stable = fixture();
  assert.deepEqual(
    await revalidateOrphanedUnstarted({
      github: stable.github,
      owner: "owner",
      repo: "repo",
      listedRun: classified.run,
      currentSha: CURRENT_SHA,
      nowMs: NOW,
    }),
    audit,
  );

  const changedFixtures = [
    fixture({
      liveRun: {
        status: "in_progress",
        updated_at: "2026-07-30T10:37:28Z",
      },
    }),
    fixture({
      responses: {
        jobs: {data: {total_count: 1, jobs: [{id: 99}]}},
      },
    }),
    fixture({
      responses: {
        workflow: {
          data: {path: BUILD_FEED_WORKFLOW, state: "active"},
        },
      },
    }),
  ];
  for (const changed of changedFixtures) {
    await assert.rejects(
      () =>
        revalidateOrphanedUnstarted({
          github: changed.github,
          owner: "owner",
          repo: "repo",
          listedRun: classified.run,
          currentSha: CURRENT_SHA,
          nowMs: NOW,
        }),
      /changed after quarantine/,
    );
  }
});

test("409 and 422 are tolerated only after the same run is confirmed completed", async () => {
  const calls = [];
  const github = {
    rest: {
      actions: {
        async getWorkflowRun(input) {
          calls.push(input);
          return {
            data: {
              id: PRODUCTION_GHOST_RUN_ID,
              status: "completed",
            },
          };
        },
      },
    },
  };
  for (const status of [409, 422]) {
    await confirmTerminalAfterCancelConflict({
      github,
      owner: "owner",
      repo: "repo",
      runId: PRODUCTION_GHOST_RUN_ID,
      error: serverError(status),
    });
  }
  assert.equal(calls.length, 2);

  github.rest.actions.getWorkflowRun = async () => ({
    data: {id: PRODUCTION_GHOST_RUN_ID, status: "queued"},
  });
  await assert.rejects(
    () =>
      confirmTerminalAfterCancelConflict({
        github,
        owner: "owner",
        repo: "repo",
        runId: PRODUCTION_GHOST_RUN_ID,
        error: serverError(409),
      }),
    /server 409/,
  );
  await assert.rejects(
    () =>
      confirmTerminalAfterCancelConflict({
        github,
        owner: "owner",
        repo: "repo",
        runId: PRODUCTION_GHOST_RUN_ID,
        error: serverError(500),
      }),
    /server 500/,
  );
});
