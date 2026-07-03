import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createManifest,
  loadManifest,
  saveManifest,
  slugifySystemName,
  stateFilePath,
} from "../datatailr-system-builder/state.ts";
import type { SystemSpec } from "../datatailr-system-builder/types.ts";

test("slugifySystemName normalizes user-facing names", () => {
  assert.equal(slugifySystemName("My Data Platform"), "my_data_platform");
  assert.equal(slugifySystemName("prices/api"), "prices_api");
});

test("saveManifest persists to a project-local .pi state file", async () => {
  const cwd = await mkdtemp(join(tmpdir(), "dt-system-builder-"));
  const spec: SystemSpec = {
    slug: "market_data",
    displayName: "Market Data",
    summary: "Realtime market data system",
    multiComponent: false,
    components: [],
  };

  const manifest = createManifest(spec);
  const savedPath = await saveManifest(cwd, manifest);
  const reloaded = await loadManifest(cwd, "market_data");

  assert.equal(savedPath, stateFilePath(cwd, "market_data"));
  assert.equal(reloaded?.spec.slug, "market_data");
  assert.equal(reloaded?.version, 1);

  await rm(cwd, { recursive: true, force: true });
});
