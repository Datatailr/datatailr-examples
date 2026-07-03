import { loadManifest } from "./state.ts";
import { collectSystemSpec, type WizardUI } from "./wizard.ts";
import { scaffoldSystem } from "./scaffold.ts";
import { buildSubagentTasks, spawnSubagent, waitForSubagents } from "./subagents.ts";
import { deployComponent } from "./datatailr.ts";
import type { CommandRunner } from "./shell.ts";

export interface OrchestratorDeps {
  cwd: string;
  ui: WizardUI & { notify(message: string, level?: "info" | "warning" | "error"): void };
  runner: CommandRunner;
}

export async function runNewSystemFlow(deps: OrchestratorDeps): Promise<string> {
  const spec = await collectSystemSpec(deps.ui);
  if (!spec) return "Cancelled";

  const scaffoldResult = await scaffoldSystem(deps.cwd, spec);
  const spawned = [];
  for (const task of buildSubagentTasks(spec)) {
    spawned.push(await spawnSubagent(deps.runner, task, deps.cwd));
  }
  const completed = await waitForSubagents(deps.runner, spawned.map((item) => item.id), deps.cwd, 1000);
  for (const component of spec.components) {
    await deployComponent(deps.runner, deps.cwd, component);
  }
  deps.ui.notify(`Scaffolded ${scaffoldResult.files.length} files and deployed ${spec.components.length} component(s).`, "info");
  return completed.map((item) => `${item.title}: ${item.status ?? "unknown"}`).join("\n");
}

export async function loadSystemManifestOrThrow(cwd: string, slug: string) {
  const manifest = await loadManifest(cwd, slug);
  if (!manifest) throw new Error(`No manifest found for ${slug}`);
  return manifest;
}
