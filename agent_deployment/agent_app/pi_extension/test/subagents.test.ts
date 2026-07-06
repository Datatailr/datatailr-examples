import test from "node:test";
import assert from "node:assert/strict";
import { buildSpawnSubagentArgs, buildSubagentTasks, checkSubagents } from "../datatailr-system-builder/subagents.ts";
import type { CommandRunner } from "../datatailr-system-builder/shell.ts";
import type { SystemSpec } from "../datatailr-system-builder/types.ts";

test("buildSubagentTasks creates one focused task per component", () => {
  const spec: SystemSpec = {
    slug: "market_data",
    displayName: "Market Data",
    summary: "App and service",
    multiComponent: true,
    components: [
      {
        kind: "app",
        slug: "app",
        displayName: "market_data app",
        packageName: "market_data_app",
        directory: "systems/market_data/app",
        deployScript: "systems/market_data/app/deploy.py",
        jobName: "market-data-app",
      },
      {
        kind: "service",
        slug: "service",
        displayName: "market_data service",
        packageName: "market_data_service",
        directory: "systems/market_data/service",
        deployScript: "systems/market_data/service/deploy.py",
        jobName: "market-data-service",
      },
    ],
  };

  const tasks = buildSubagentTasks(spec);
  assert.equal(tasks.length, 2);
  assert.match(tasks[0].title, /Build Market Data app/i);
  assert.deepEqual(tasks[0].files, ["systems/market_data/app"]);
});

test("buildSpawnSubagentArgs renders repeated --done flags and file hints", () => {
  const args = buildSpawnSubagentArgs({
    title: "Build app",
    instructions: "Implement Flask app",
    done: ["app runs", "tests pass"],
    files: ["systems/market_data/app"],
  });

  assert.deepEqual(args, [
    "--title", "Build app",
    "--instructions", "Implement Flask app",
    "--done", "app runs",
    "--done", "tests pass",
    "--files", "systems/market_data/app",
  ]);
});

test("checkSubagents parses check_subagents --json output", async () => {
  const runner: CommandRunner = {
    async exec() {
      return {
        stdout: JSON.stringify({
          parent_id: "req-1",
          subagents: [{
            subagent_id: "req-1.001.abc",
            title: "Build app",
            status: "succeeded",
            pr_url: "https://example.test/pr/1"
          }]
        }),
        stderr: "",
        exitCode: 0,
      };
    },
  };

  const results = await checkSubagents(runner, undefined, "/repo");
  assert.equal(results[0].id, "req-1.001.abc");
  assert.equal(results[0].status, "succeeded");
  assert.equal(results[0].prUrl, "https://example.test/pr/1");
});
