import type { CommandRunner } from "./shell.ts";
import type { ComponentSpec, SubagentRecord, SystemSpec } from "./types.ts";

export interface SubagentTask {
  title: string;
  instructions: string;
  done: string[];
  files: string[];
}

function titleForComponent(spec: SystemSpec, component: ComponentSpec): string {
  return `Build ${spec.displayName} ${component.kind}`;
}

export function buildSubagentTasks(spec: SystemSpec): SubagentTask[] {
  return spec.components.map((component) => ({
    title: titleForComponent(spec, component),
    instructions: `Implement the ${component.kind} component for ${spec.displayName}. Keep work focused on ${component.directory} and ensure the generated deploy script remains valid for Datatailr.`,
    done: [
      `${component.kind} implementation completed`,
      `${component.deployScript} remains deployable`,
      `tests for ${component.kind} pass`,
    ],
    files: [component.directory],
  }));
}

export function buildSpawnSubagentArgs(task: SubagentTask): string[] {
  return [
    "--title", task.title,
    "--instructions", task.instructions,
    ...task.done.flatMap((item) => ["--done", item]),
    ...(task.files.length > 0 ? ["--files", task.files.join(",")] : []),
  ];
}

export async function spawnSubagent(runner: CommandRunner, task: SubagentTask, cwd: string): Promise<SubagentRecord> {
  const result = await runner.exec("spawn_subagent", buildSpawnSubagentArgs(task), { cwd });
  if (result.exitCode !== 0) throw new Error(result.stderr || result.stdout);
  const parsed = JSON.parse(result.stdout) as { subagent_id: string; branch?: string; pr_url?: string | null };
  return { id: parsed.subagent_id, title: task.title, branch: parsed.branch, prUrl: parsed.pr_url ?? null };
}

export async function checkSubagents(runner: CommandRunner, id?: string, cwd?: string): Promise<SubagentRecord[]> {
  const args = ["--json", ...(id ? ["--id", id] : [])];
  const result = await runner.exec("check_subagents", args, { cwd });
  if (result.exitCode !== 0) throw new Error(result.stderr || result.stdout);
  const parsed = JSON.parse(result.stdout) as {
    subagents?: Array<{ subagent_id: string; title: string; status?: string; pr_url?: string | null }>;
  };
  return (parsed.subagents ?? []).map((item) => ({
    id: item.subagent_id,
    title: item.title,
    status: item.status,
    prUrl: item.pr_url ?? null,
  }));
}

export async function waitForSubagents(
  runner: CommandRunner,
  ids: string[],
  cwd: string,
  sleepMs = 20_000,
): Promise<SubagentRecord[]> {
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
  while (true) {
    const all = await checkSubagents(runner, undefined, cwd);
    const matching = all.filter((record) => ids.includes(record.id));
    if (matching.every((record) => record.status && record.status !== "-")) return matching;
    await sleep(sleepMs);
  }
}
