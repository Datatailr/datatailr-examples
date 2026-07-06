export type DtSystemAction = "new" | "scaffold" | "delegate" | "deploy" | "status" | "logs";

export function parseDtSystemArgs(input: string): { action: DtSystemAction; target?: string } {
  const [actionRaw, target] = input.trim().split(/\s+/, 2);
  const action = (actionRaw || "new") as DtSystemAction;
  return target ? { action, target } : { action };
}
