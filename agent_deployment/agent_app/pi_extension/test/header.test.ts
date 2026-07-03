import test from "node:test";
import assert from "node:assert/strict";
import {
  buildHeaderLines,
  discoverContextFiles,
  type HeaderData,
  type HeaderTheme,
} from "../datatailr-system-builder/header.ts";

// A pass-through theme that tags each segment so tests can assert on color use
// without depending on real ANSI escape codes.
const theme: HeaderTheme = {
  fg: (color, text) => `<${color}>${text}</${color}>`,
};

function plain(lines: string[]): string {
  return lines.join("\n").replace(/<\/?[a-z]+>/g, "");
}

const sampleData: HeaderData = {
  cwd: "/repo",
  home: "/home/dev",
  contextFiles: ["/home/dev/.pi/agent/AGENTS.md", "/repo/AGENTS.md"],
  tools: [
    { name: "read", sourceInfo: { source: "builtin" } },
    { name: "spawn_subagent", sourceInfo: { source: "subagent-tools" } },
  ],
  commands: [
    { name: "review", source: "prompt", sourceInfo: { path: "/prompts/review.md", source: "review" } },
    { name: "skill:blob-storage", source: "skill", sourceInfo: { path: "/skills/blob", source: "blob" } },
    { name: "skill:deploy-app", source: "skill", sourceInfo: { path: "/skills/app", source: "app" } },
  ],
};

test("buildHeaderLines renders both ASCII logos on the same rows when wide", () => {
  const wide = buildHeaderLines(sampleData, theme, 120);
  const text = plain(wide);
  assert.ok(text.includes("█"), "expected ASCII block art in the header");
  assert.ok(text.includes("datatailr × pi coding agent"), "expected the datatailr/pi tagline");
  // A row that contains both the last banner column glyph and the mascot leg
  // confirms the two logos share a line.
  const combined = wide.find((l) => l.includes("██") && l.includes("  ██  ██"));
  assert.ok(combined, "expected a row with the Datatailr banner and pi mascot side by side");
});

test("buildHeaderLines lists loaded resources with counts", () => {
  const text = plain(buildHeaderLines(sampleData, theme, 120));
  assert.match(text, /Skills \(2\)/);
  assert.match(text, /Prompts \(1\)/);
  assert.match(text, /Context \(2\)/);
  assert.ok(text.includes("skill:blob-storage"), "expected skill names listed");
});

test("buildHeaderLines always lists this package as a loaded extension", () => {
  const text = plain(buildHeaderLines(sampleData, theme, 120));
  assert.match(text, /Extensions \(\d+\)/);
  assert.ok(text.includes("datatailr-system-builder"), "expected the datatailr extension listed");
});

test("buildHeaderLines includes extension-provided tools as extensions", () => {
  const text = plain(buildHeaderLines(sampleData, theme, 120));
  assert.ok(text.includes("subagent-tools"), "expected non-builtin tool source listed as extension");
  assert.ok(!/Extensions \([^)]*\).*\bread\b/.test(text), "built-in tools must not be listed as extensions");
});

test("buildHeaderLines shows the global agent AGENTS.md with a ~ prefix", () => {
  const text = plain(buildHeaderLines(sampleData, theme, 120));
  assert.ok(text.includes("~/.pi/agent/AGENTS.md"), "expected the global agent context file with ~ prefix");
  assert.ok(!text.includes("/home/dev/.pi/agent/AGENTS.md"), "expected home prefix collapsed to ~");
});

test("buildHeaderLines shows (none) for empty resource groups", () => {
  const text = plain(buildHeaderLines({ commands: [], tools: [], contextFiles: [], cwd: "/repo" }, theme, 120));
  assert.match(text, /Skills \(0\)/);
  assert.match(text, /Context \(0\)/);
  assert.ok(text.includes("(none)"));
});

test("buildHeaderLines falls back to a compact logo on narrow terminals", () => {
  const narrow = plain(buildHeaderLines(sampleData, theme, 30));
  assert.ok(narrow.includes("≋ datatailr × pi"), "expected compact wordmark when width is small");
});

test("buildHeaderLines truncates long item lists to fit the width", () => {
  const many: HeaderData = {
    cwd: "/repo",
    contextFiles: [],
    tools: [],
    commands: Array.from({ length: 30 }, (_, i) => ({
      name: `skill:really-long-skill-name-${i}`,
      source: "skill" as const,
      sourceInfo: { path: `/skills/${i}`, source: `${i}` },
    })),
  };
  const lines = buildHeaderLines(many, theme, 80);
  const skillLine = lines.find((l) => l.includes("Skills ("));
  assert.ok(skillLine, "expected a Skills line");
  const visibleWidth = skillLine!.replace(/<\/?[a-z]+>/g, "").length;
  assert.ok(visibleWidth <= 80, `Skills line should fit width, got ${visibleWidth}`);
  assert.match(plain([skillLine!]), /\+\d+ more/);
});

test("discoverContextFiles mirrors pi: global agent file then cwd walk to root", () => {
  const present = new Set([
    "/home/dev/.pi/agent/AGENTS.md",
    "/repo/AGENTS.md",
    "/repo/team/CLAUDE.md",
  ]);
  const files = discoverContextFiles({
    cwd: "/repo/team/project",
    agentDir: "/home/dev/.pi/agent",
    exists: (p) => present.has(p),
  });
  assert.deepEqual(files, [
    "/home/dev/.pi/agent/AGENTS.md", // global first
    "/repo/AGENTS.md", // ancestors top-down
    "/repo/team/CLAUDE.md",
  ]);
});

test("discoverContextFiles prefers AGENTS.md over CLAUDE.md within a directory", () => {
  const present = new Set(["/repo/AGENTS.md", "/repo/CLAUDE.md"]);
  const files = discoverContextFiles({
    cwd: "/repo",
    agentDir: "/nope",
    exists: (p) => present.has(p),
  });
  assert.deepEqual(files, ["/repo/AGENTS.md"]);
});
