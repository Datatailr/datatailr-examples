import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildComponentFiles, defaultComponentSpec, scaffoldSystem } from "../src/scaffold.ts";
import type { SystemSpec } from "../src/types.ts";

test("buildComponentFiles emits Flask app and service, workflow, and excel layouts", () => {
  const appFiles = buildComponentFiles(defaultComponentSpec("market_data", "app"));
  const serviceFiles = buildComponentFiles(defaultComponentSpec("market_data", "service"));
  const workflowFiles = buildComponentFiles(defaultComponentSpec("market_data", "workflow"));
  const excelFiles = buildComponentFiles(defaultComponentSpec("market_data", "excel"));

  assert.ok(appFiles.some((file) => file.path.endsWith("app/deploy.py") && file.content.includes("framework=\"flask\"")));
  assert.ok(serviceFiles.some((file) => file.content.includes('def main(port):')));
  assert.ok(workflowFiles.some((file) => file.content.includes("@workflow(name=")));
  assert.ok(excelFiles.some((file) => file.content.includes('addin = Addin(')));
});

test("scaffoldSystem writes a multi-component system and saves manifest", async () => {
  const cwd = await mkdtemp(join(tmpdir(), "dt-scaffold-"));
  const spec: SystemSpec = {
    slug: "market_data",
    displayName: "Market Data",
    summary: "Combined app and service",
    multiComponent: true,
    components: [
      defaultComponentSpec("market_data", "app"),
      defaultComponentSpec("market_data", "service"),
    ],
  };

  const result = await scaffoldSystem(cwd, spec);
  const deployPy = await readFile(join(cwd, "systems", "market_data", "app", "deploy.py"), "utf8");

  assert.ok(result.files.includes(join("systems", "market_data", "service", "deploy.py")));
  assert.ok(result.manifestPath.endsWith(".pi/datatailr-system-builder/market_data.json"));
  assert.match(deployPy, /from market_data_app\.app import app/);

  await rm(cwd, { recursive: true, force: true });
});
