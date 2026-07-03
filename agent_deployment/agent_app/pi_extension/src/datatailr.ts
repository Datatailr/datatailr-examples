import { basename, dirname } from "node:path";
import type { CommandRunner } from "./shell.ts";
import type { ComponentSpec } from "./types.ts";

export function buildDeployInvocation(component: ComponentSpec): { command: string; args: string[]; cwd: string } {
  return {
    command: "python",
    args: [basename(component.deployScript)],
    cwd: dirname(component.deployScript),
  };
}

export function buildJobGetArgs(jobName: string, env?: string): string[] {
  return ["job", "get", jobName, ...(env ? ["-e", env] : []), "--json"];
}

export function buildLogReadArgs(
  jobName: string,
  options: { env?: string; lines?: number; stderr?: boolean } = {},
): string[] {
  return [
    "log",
    "read",
    jobName,
    ...(options.lines ? ["-l", String(options.lines)] : []),
    ...(options.env ? ["-e", options.env] : []),
    ...(options.stderr ? ["-r"] : []),
  ];
}

export async function deployComponent(runner: CommandRunner, repoRoot: string, component: ComponentSpec): Promise<void> {
  const invocation = buildDeployInvocation(component);
  const result = await runner.exec(invocation.command, invocation.args, { cwd: `${repoRoot}/${invocation.cwd}` });
  if (result.exitCode !== 0) throw new Error(result.stderr || result.stdout);
}
