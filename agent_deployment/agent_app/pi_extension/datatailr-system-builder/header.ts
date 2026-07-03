import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import type { ExtensionAPI, Theme } from "@earendil-works/pi-coding-agent";

/**
 * Startup header for the Datatailr pi package.
 *
 * Replaces pi's built-in header with the datatailr figlet wordmark plus a live
 * inventory of everything pi loaded for the session: skills, extensions, prompt
 * templates, and context files. The rendering lives in the pure
 * `buildHeaderLines` function so it can be unit tested without a TUI.
 */

/** Minimal shape of a slash command as returned by `pi.getCommands()`. */
export interface HeaderCommand {
  name: string;
  description?: string;
  source: "extension" | "prompt" | "skill";
  sourceInfo?: { path?: string; source?: string };
}

/** Minimal shape of a tool as returned by `pi.getAllTools()`. */
export interface HeaderTool {
  name: string;
  sourceInfo?: { path?: string; source?: string };
}

/** Everything the header needs to render, gathered lazily on each redraw. */
export interface HeaderData {
  commands: HeaderCommand[];
  tools: HeaderTool[];
  contextFiles: string[];
  cwd?: string;
  home?: string;
}

/** The slice of pi's `Theme` the header relies on (keeps rendering testable). */
export interface HeaderTheme {
  fg(color: string, text: string): string;
}

const INDENT = "  ";
const LABEL_WIDTH = 14;
const SEPARATOR = " · ";

/** This header ships inside this package, so the package is always loaded. */
const SELF_EXTENSION = "datatailr-system-builder";

/** pi's built-in tools; anything else in `getAllTools()` is extension-provided. */
const BUILTIN_TOOLS = new Set(["read", "write", "edit", "bash", "grep", "find", "ls"]);

// pi env var for the agent config dir (see config.ts: `${APP_NAME}_CODING_AGENT_DIR`).
const ENV_AGENT_DIR = "PI_CODING_AGENT_DIR";

// "datatailr" figlet wordmark (slant/standard style).
const DATATAILR_BANNER = [
  "     _       _         _        _ _                       _ ",
  "  __| | __ _| |_ __ _ | |_ __ _(_) |_ __            _ __ (_)",
  " / _` |/ _` | __/ _` || __/ _` | | | '__|  _____   | '_ \\| |",
  "| (_| | (_| | || (_| || || (_| | | | |    |_____|  | |_) | |",
  " \\__,_|\\__,_|\\__\\__,_(_)__\\__,_|_|_|_|             | .__/|_|",
  "                                                   |_|      ",
];

const DATATAILR_BANNER_WIDTH = Math.max(...DATATAILR_BANNER.map((l) => l.length));

/** Logo lines: full figlet wordmark, or a compact fallback on narrow terminals. */
function logoLines(width: number): string[] {
  if (width >= INDENT.length + DATATAILR_BANNER_WIDTH) {
    return [...DATATAILR_BANNER];
  }
  return ["≋ datatailr × pi"];
}

/** Join item labels with commas, truncating to fit `maxWidth` display columns. */
function joinItems(items: string[], maxWidth: number): string {
  if (items.length === 0) {
    return "(none)";
  }
  const out: string[] = [];
  let used = 0;
  for (let i = 0; i < items.length; i++) {
    const piece = out.length === 0 ? items[i] : `, ${items[i]}`;
    const remaining = items.length - i;
    const suffix = remaining > 1 ? ` +${remaining - 1} more` : "";
    if (used + piece.length + suffix.length > maxWidth && out.length > 0) {
      out.push(` +${remaining} more`);
      return out.join("");
    }
    out.push(piece);
    used += piece.length;
  }
  return out.join("");
}

function baseName(raw: string): string {
  return raw.split(/[/\\]/).filter(Boolean).pop() ?? raw;
}

/**
 * Loaded extension names. `getCommands()` (source "extension") is the ideal
 * source, but not every pi build surfaces extension commands there, so we also
 * fold in extension-provided tools from `getAllTools()` and always include this
 * package (whose extension is, by definition, loaded).
 */
function extensionNames(commands: HeaderCommand[], tools: HeaderTool[]): string[] {
  const names = new Set<string>([SELF_EXTENSION]);

  for (const command of commands) {
    if (command.source !== "extension") {
      continue;
    }
    const raw = command.sourceInfo?.source || command.sourceInfo?.path || command.name;
    names.add(baseName(raw));
  }

  for (const tool of tools) {
    if (BUILTIN_TOOLS.has(tool.name)) {
      continue;
    }
    const source = tool.sourceInfo?.source;
    if (source && source !== "builtin" && source !== "core") {
      names.add(baseName(source));
    } else if (tool.sourceInfo?.path) {
      names.add(baseName(tool.sourceInfo.path));
    }
  }

  return [...names];
}

function commandNames(commands: HeaderCommand[], source: HeaderCommand["source"]): string[] {
  return commands.filter((c) => c.source === source).map((c) => c.name);
}

/** Human-friendly path: `~`-relative for home, cwd-relative for the project. */
function displayPath(path: string, cwd?: string, home?: string): string {
  if (cwd && (path === cwd || path.startsWith(`${cwd}/`) || path.startsWith(`${cwd}\\`))) {
    return path.slice(cwd.length).replace(/^[/\\]/, "") || ".";
  }
  if (home && (path === home || path.startsWith(`${home}/`) || path.startsWith(`${home}\\`))) {
    return `~${path.slice(home.length)}`;
  }
  const parts = path.split(/[/\\]/).filter(Boolean);
  return parts.length > 3 ? `…/${parts.slice(-2).join("/")}` : path;
}

/**
 * Build the full header as an array of (already colored) terminal lines.
 * Pure and deterministic so it can be exercised in unit tests.
 */
export function buildHeaderLines(data: HeaderData, theme: HeaderTheme, width: number): string[] {
  const accent = (t: string) => theme.fg("accent", t);
  const muted = (t: string) => theme.fg("muted", t);
  const dim = (t: string) => theme.fg("dim", t);
  const text = (t: string) => theme.fg("text", t);

  const lines: string[] = [""];

  // --- Logo --------------------------------------------------------------
  for (const line of logoLines(width)) {
    lines.push(accent(INDENT + line));
  }
  lines.push(`${INDENT}${dim("datatailr")}${muted(" × ")}${accent("pi coding agent")}`);
  lines.push("");

  // --- Loaded resources --------------------------------------------------
  const skills = commandNames(data.commands, "skill");
  const extensions = extensionNames(data.commands, data.tools);
  const prompts = commandNames(data.commands, "prompt");
  const contextFiles = data.contextFiles.map((p) => displayPath(p, data.cwd, data.home));

  const ruleWidth = Math.max(0, Math.min(width, DATATAILR_BANNER_WIDTH) - INDENT.length);
  lines.push(`${INDENT}${dim("─".repeat(ruleWidth))}`);
  lines.push(`${INDENT}${text("Loaded for this session")}`);

  const sections: Array<[string, string[]]> = [
    ["Skills", skills],
    ["Extensions", extensions],
    ["Prompts", prompts],
    ["Context", contextFiles],
  ];

  const itemsWidth = Math.max(12, width - INDENT.length - LABEL_WIDTH - SEPARATOR.length);
  for (const [name, items] of sections) {
    const label = `${name} (${items.length})`.padEnd(LABEL_WIDTH);
    lines.push(`${INDENT}${accent(label)}${dim(SEPARATOR)}${muted(joinItems(items, itemsWidth))}`);
  }

  lines.push("");
  lines.push(
    `${INDENT}${dim("Type ")}${accent("/dt-system")}${dim(" to build a Datatailr system · ")}${accent("/builtin-header")}${dim(" to restore the default header")}`,
  );
  lines.push("");

  return lines;
}

/**
 * Replicate pi's context-file discovery (see resource-loader.ts
 * `loadProjectContextFiles`): the global `AGENTS.md`/`CLAUDE.md` in the agent
 * dir, then the nearest such file walking from `cwd` up to the filesystem root.
 * Reproduced here because the loaded set is not exposed to extensions, yet the
 * built-in header shows it (including `~/.pi/agent/AGENTS.md`).
 */
export function discoverContextFiles(opts: {
  cwd: string;
  agentDir: string;
  exists?: (path: string) => boolean;
}): string[] {
  const exists = opts.exists ?? existsSync;
  const candidates = ["AGENTS.md", "AGENTS.MD", "CLAUDE.md", "CLAUDE.MD"];
  const firstIn = (dir: string): string | null => {
    for (const name of candidates) {
      const filePath = join(dir, name);
      if (exists(filePath)) {
        return filePath;
      }
    }
    return null;
  };

  const result: string[] = [];
  const seen = new Set<string>();

  const global = firstIn(opts.agentDir);
  if (global) {
    result.push(global);
    seen.add(global);
  }

  const ancestors: string[] = [];
  let current = resolve(opts.cwd);
  const root = resolve("/");
  while (true) {
    const file = firstIn(current);
    if (file && !seen.has(file)) {
      ancestors.unshift(file);
      seen.add(file);
    }
    if (current === root) {
      break;
    }
    const parent = dirname(current);
    if (parent === current) {
      break;
    }
    current = parent;
  }

  result.push(...ancestors);
  return result;
}

function resolveAgentDir(home: string): string {
  const envDir = process.env[ENV_AGENT_DIR];
  if (envDir) {
    return envDir.startsWith("~") ? join(home, envDir.slice(1)) : envDir;
  }
  return join(home, ".pi", "agent");
}

/**
 * Register the custom startup header plus commands to show it (`/dt-header`)
 * and restore pi's built-in header (`/builtin-header`). Skills, extensions and
 * prompts are read live on every redraw from `pi.getCommands()`/`getAllTools()`;
 * context files are discovered from disk at `session_start`.
 */
export function registerStartupHeader(pi: ExtensionAPI): void {
  const home = process.env.HOME || homedir();
  const agentDir = resolveAgentDir(home);
  let contextFiles: string[] = [];
  let cwd: string | undefined;

  const getCommands = (): HeaderCommand[] => {
    try {
      return pi.getCommands() as unknown as HeaderCommand[];
    } catch {
      return [];
    }
  };
  const getTools = (): HeaderTool[] => {
    try {
      return (pi.getAllTools?.() ?? []) as unknown as HeaderTool[];
    } catch {
      return [];
    }
  };

  const getData = (): HeaderData => ({
    commands: getCommands(),
    tools: getTools(),
    contextFiles,
    cwd,
    home,
  });

  const headerFactory = (_tui: unknown, theme: Theme) => ({
    render(renderWidth: number): string[] {
      return buildHeaderLines(getData(), theme as unknown as HeaderTheme, renderWidth);
    },
    invalidate() {},
  });

  const showHeader = (ctxCwd: string, setHeader: (factory: unknown) => void) => {
    cwd = ctxCwd;
    contextFiles = discoverContextFiles({ cwd: ctxCwd, agentDir });
    setHeader(headerFactory);
  };

  pi.on("session_start", async (_event, ctx) => {
    if (ctx.mode !== "tui") {
      return;
    }
    showHeader(ctx.cwd, (factory) => ctx.ui.setHeader(factory as never));
  });

  pi.registerCommand("builtin-header", {
    description: "Restore pi's built-in header",
    handler: async (_args, ctx) => {
      ctx.ui.setHeader(undefined);
      ctx.ui.notify("Built-in header restored", "info");
    },
  });

  pi.registerCommand("dt-header", {
    description: "Show the Datatailr startup header",
    handler: async (_args, ctx) => {
      if (ctx.mode !== "tui") {
        ctx.ui.notify("The Datatailr header is only available in the terminal UI.", "info");
        return;
      }
      showHeader(ctx.cwd, (factory) => ctx.ui.setHeader(factory as never));
      ctx.ui.notify("Datatailr header restored", "info");
    },
  });
}
