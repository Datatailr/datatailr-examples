import test from "node:test";
import assert from "node:assert/strict";
import { buildDeployInvocation, buildJobGetArgs, buildLogReadArgs } from "../src/datatailr.ts";
import type { ComponentSpec } from "../src/types.ts";

const appComponent: ComponentSpec = {
  kind: "app",
  slug: "app",
  displayName: "market data app",
  packageName: "market_data_app",
  directory: "systems/market_data/app",
  deployScript: "systems/market_data/app/deploy.py",
  jobName: "market-data-app",
};

test("buildDeployInvocation runs python deploy.py from the component directory", () => {
  assert.deepEqual(buildDeployInvocation(appComponent), {
    command: "python",
    args: ["deploy.py"],
    cwd: "systems/market_data/app",
  });
});

test("buildJobGetArgs targets the requested environment", () => {
  assert.deepEqual(buildJobGetArgs("market-data-app", "prod"), ["job", "get", "market-data-app", "-e", "prod", "--json"]);
});

test("buildLogReadArgs supports line counts and stderr", () => {
  assert.deepEqual(buildLogReadArgs("market-data-app", { lines: 100, stderr: true }), ["log", "read", "market-data-app", "-l", "100", "-r"]);
});
