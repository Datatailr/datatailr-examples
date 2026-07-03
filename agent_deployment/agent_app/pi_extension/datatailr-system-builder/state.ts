import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { SystemManifest, SystemSpec } from "./types.ts";

export function slugifySystemName(name: string): string {
  return name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "datatailr_system";
}

export function stateFilePath(cwd: string, systemSlug: string): string {
  return join(cwd, ".pi", "datatailr-system-builder", `${systemSlug}.json`);
}

export function createManifest(spec: SystemSpec): SystemManifest {
  return {
    version: 1,
    spec,
    generatedAt: new Date().toISOString(),
    subagents: [],
    jobs: spec.components.map((component) => ({
      componentSlug: component.slug,
      kind: component.kind,
      jobName: component.jobName,
      deployScript: component.deployScript,
    })),
  };
}

export async function saveManifest(cwd: string, manifest: SystemManifest): Promise<string> {
  const path = stateFilePath(cwd, manifest.spec.slug);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(manifest, null, 2));
  return path;
}

export async function loadManifest(cwd: string, systemSlug: string): Promise<SystemManifest | undefined> {
  const path = stateFilePath(cwd, systemSlug);
  try {
    const raw = await readFile(path, "utf8");
    return JSON.parse(raw) as SystemManifest;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}
