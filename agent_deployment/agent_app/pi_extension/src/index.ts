import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { parseDtSystemArgs } from "./commands.ts";
import { runNewSystemFlow } from "./orchestrator.ts";
import type { CommandRunner } from "./shell.ts";

function createRunner(pi: ExtensionAPI): CommandRunner {
  return {
    async exec(command, args, options) {
      const result = await pi.exec(command, args, { cwd: options?.cwd });
      return {
        stdout: result.stdout,
        stderr: result.stderr,
        exitCode: result.code,
      };
    },
  };
}

export function registerDatatailrSystemBuilder(pi: ExtensionAPI): void {
  pi.registerCommand("dt-system", {
    description: "Scaffold, delegate, deploy, and inspect Datatailr systems",
    handler: async (args, ctx) => {
      const parsed = parseDtSystemArgs(args);
      const runner = createRunner(pi);

      if (parsed.action === "new") {
        await runNewSystemFlow({
          cwd: ctx.cwd,
          runner,
          ui: {
            input: (title, placeholder) => ctx.ui.input(title, placeholder),
            select: (title, options) => ctx.ui.select(title, options),
            editor: (title, value) => ctx.ui.editor(title, value ?? ""),
            notify: (message, level = "info") => ctx.ui.notify(message, level),
          },
        });
        return;
      }

      ctx.ui.notify(`/${"dt-system"} ${parsed.action} is implemented in the next task branch.`, "info");
    },
  });
}
