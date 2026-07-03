import { slugifySystemName } from "./state.ts";
import { defaultComponentSpec } from "./scaffold.ts";
import type { ComponentKind, SystemSpec } from "./types.ts";

export interface WizardUI {
  input(title: string, placeholder?: string): Promise<string | undefined>;
  select(title: string, options: string[]): Promise<string | undefined>;
  editor(title: string, value?: string): Promise<string | undefined>;
}

export async function collectSystemSpec(ui: WizardUI): Promise<SystemSpec | undefined> {
  const displayName = await ui.input("System name", "Market Data");
  if (!displayName) return undefined;

  const componentKinds = await ui.input("Component kinds", "app,service");
  if (!componentKinds) return undefined;

  const summary = await ui.editor("System summary", "Describe the Datatailr system");
  const slug = slugifySystemName(displayName);
  const kinds = componentKinds.split(",").map((item) => item.trim()).filter(Boolean) as ComponentKind[];

  return {
    slug,
    displayName,
    summary: summary?.trim() || displayName,
    multiComponent: kinds.length > 1,
    components: kinds.map((kind) => defaultComponentSpec(slug, kind)),
  };
}
